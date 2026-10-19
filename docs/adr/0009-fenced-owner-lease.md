# ADR 0009: The fenced owner lease

## Status

Proposed (issue #2250). Epic #2247.

This ADR holds the lease rules that the model forces. Issue #2249 adds the
partition rules: the partition count, the hash input, routing and the lock
order across the generation, owner and execution rows.

## Context

Every 2026 bug of the actor-ownership survey (#2242) is on the
ownership-change seam:

- **golem#3766, golem#3845.** A lease with no commit fence lets a paused
  executor finish its writes.
- **restate#4566.** A leader change drops signals that HTTP 202 already
  acknowledged.
- **restate#5033.** After a failed seal, leaders accept appends but stop
  applying the log.

The [research report](../../reports/Actor%20ownership%20prior%20art%20for%20Harvest.md)
proposes the protocol. Issue #2250 asks for a spec before the code. Two
models check it:

- `formal/tla/OwnerLease.tla`, with TLC. See
  [`formal-methods.md`](../testing/formal-methods.md).
- `autumn_harvest::dst::owner`, a seeded simulator. See
  [`simulation.md`](../testing/simulation.md).

Both run four nemeses:

1. A stale owner commits after a takeover.
2. The lease flaps during an append.
3. A renewal happens while commits are in flight.
4. A signal is acknowledged just before an epoch bump.

## Decision

Each decision partition has one owner row in `harvest_decision_owner`:
`(partition, holder, epoch, lease_until)`. The row is in the same database
as the history that it fences. `partition` is the primary key. No other
unique index covers `holder`, `epoch` or `lease_until`.

The rules below hold. Each rule has a mutant that breaks it. TLC and the
simulator must reject each mutant on the named invariant.

| # | Rule | Mutant | Invariant |
|---|---|---|---|
| R1 | The commit checks the epoch in its own transaction. A lease check outside the commit is not a fence. | `no-fence` | `OneOwnerPerEpoch` |
| R2 | The fence reads the owner row `FOR KEY SHARE` and keeps the lock until the commit ends. A takeover runs `SELECT ... FOR UPDATE` on the row before it writes, so it waits for every commit in flight. | `unlocked-fence` | `OneOwnerPerEpoch` |
| R3 | The fence compares the epoch that the decision pinned when it began. It does not compare the holder. | `holder-fence` | `DecisionOnFreshHistory` |
| R4 | Each acquisition adds 1 to the epoch. The owner then loads its resident state, after the acquisition commits. This is also true when the same worker takes the row back. | `keep-cache` | `DecisionOnFreshHistory` |
| R5 | A renewal never changes the epoch. | `renew-bumps` | `OwnerNeverFencesItself` |
| R6 | The server acknowledges a signal only after its durable write. The owner never acknowledges from memory. | `memory-ack` | `NoAckedSignalLost` |
| R7 | A tenure runs one decision of an execution at a time. The next decision starts from the state after the last commit. | `pipeline` | `DecisionOnFreshHistory` |

The invariants:

- `OneOwnerPerEpoch`: each event comes from the worker that took its
  epoch. So at most one owner commits at one epoch.
- `CommitAtPinnedEpoch`: a decision commits only at the epoch it pinned.
- `DecisionOnFreshHistory`: each decision saw every earlier event. So no
  event is lost.
- `NoDuplicateEvent`: no two events consume one signal.
- `AckAfterDurableWrite`: a signal is acknowledged only after its durable
  write.
- `NoAckedSignalLost`: no acknowledged signal is lost. A signal in the
  memory of an owner that lost the row counts as lost, because that owner
  can never commit it.
- `OwnerNeverFencesItself`: only a takeover fences a commit. This is an
  availability rule, not a safety rule.

### Spec changes that the model forces

The report proposed the protocol. The model makes these points exact. Each
point says what checks it.

1. **The epoch comes from the decision (R3).** The report says that a
   persist compares "the epoch it pinned at acquisition". A worker can lose
   the row and take it back while an old decision is in flight. That
   decision must carry its own epoch. A check of the holder lets it commit
   on a stale history. The `holder-fence` mutant checks this point.
2. **A re-acquisition is a cold start (R4).** The same worker that takes
   the row back must drop its resident state. Its old state can miss the
   events of another owner between its two tenures. This is the
   restate#5033 shape: an owner that appends on state it did not refresh.
   The load must follow the acquisition commit. A load before it can miss
   the last commit of the old owner. The `keep-cache` mutant checks the
   reload. The model makes the acquisition and the load one step, so it
   assumes the order.
3. **One decision at a time per tenure (R7).** Two decisions of one tenure
   read one resident state. The second one commits on a history that does
   not hold the first one. A decision of an old tenure can still run
   beside a new tenure, and the fence stops it. The `pipeline` mutant
   checks this point.
4. **A fenced commit unloads only its own tenure.** When a fenced decision
   pinned an older epoch than the worker holds now, the worker drops that
   decision only. It unloads the partition only when it still pins the
   fenced epoch. No mutant checks this point.
5. **The renewal must not wait on the commit fence.** A fence with
   `FOR SHARE` blocks a renewal `UPDATE`, so a busy owner can lose its
   lease while it commits. R2 gives the lock modes that prevent this:
   - The fence takes `FOR KEY SHARE`.
   - The renewal is an `UPDATE` of `lease_until` only. It changes no key
     column, so it takes `FOR NO KEY UPDATE`. That lock does not conflict
     with `FOR KEY SHARE`.
   - An `UPDATE` of `holder` and `epoch` also takes only
     `FOR NO KEY UPDATE`. So the takeover must run `SELECT ... FOR UPDATE`
     first. `FOR UPDATE` conflicts with `FOR KEY SHARE`.

   The model assumes these lock modes: `Renew` has no lock guard, and
   `Acquire` waits for every fence lock. No mutant checks the lock modes.
6. **A renewal must not bump the epoch (R5).** A drain of commits before a
   bump is not sufficient. A decision that read its state but did not lock
   the row still pins the old epoch. The bump fences it. The
   `renew-bumps` mutant checks this point.
7. **A lease can end during an append.** The clock does not wait for a
   lock. The append still lands at its own epoch, because the takeover
   waits for the lock. So the fence checks the epoch, not `lease_until`.
   The reach witness shows this case.

The model has no event-id backstop. The `UNIQUE (workflow_exec_id,
event_id)` check and `MAX(event_id)` stay in the code as a backstop. The
fence alone keeps every invariant.

### Liveness

The models check safety only. Two liveness rules follow from R2:

- A paused owner can hold `FOR KEY SHARE` in an open transaction. The
  takeover `FOR UPDATE` then waits for ever. The commit transaction sets
  `idle_in_transaction_session_timeout` and `lock_timeout`, so the lock
  ends.
- The takeover sets `lock_timeout` too. A lock miss is a failed attempt,
  and the worker tries again on its next tick.

## Consequences

- Stage 1a (#2251) implements `dst::owner::OwnerStore` on Postgres. It runs
  the four nemeses through `dst::owner::run_with` or
  `dst::owner::sweep_with`. The test
  `stage_1a_owner_store_gets_a_simulator_run` fails when a migration adds
  `harvest_decision_owner` and no test outside `tests/dst` implements
  `OwnerStore` for a real store and drives it.
- The simulator applies the owner rules R4, R6 and R7 itself. The
  `OwnerStore` seam makes only the store side real. When stage 1a puts its
  owner logic in its own code, that code must keep these rules, or add
  hooks to the simulator.
- A change to a rule above must change the TLA+ spec, the simulator and
  this table in the same PR.
- Foreign writers (#2253), partition routing (#2252) and failover tests
  (#2254) are out of scope here.
