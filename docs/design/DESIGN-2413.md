# DESIGN-2413: Fence the reshard override carry, and keep a clear across a retry

Issue #2413. Parent epic #2384. Follow-up to #2389 (PR #2398).

## Problem

`carry_misplaced_overrides` moves a DAG override to the shard that now owns
the DAG. The move has two gaps.

1. **No DR fence.** The carry runs after the source pass fence drops. The
   destination connection is a plain checkout. No transaction asserts the
   generation fence. A process that lost authority can still insert on the
   destination, or delete on the source, after a promotion.
2. **A retry undoes a clear.** The copy commits, and then the source delete
   fails. Both shards hold the row. An operator clears the override on the
   new owner. The next retry finds no row there and copies the stale row
   back.

## Facts

- The copy and the delete are two transactions on two databases. No single
  transaction can cover both.
- The copy keeps the stored `updated_at` of the row. The delete matches it.
  So `updated_at` already acts as the version of a moved row.
- `updated_at` comes from the clock of the writing process. Two processes
  can disagree on the time.
- `begin_fenced_groups` opens the guards of several shards as one
  operation. It takes every guard slot at once. The completion trigger relay
  uses it for its two shards.
- `run_fenced_pass` ends the backend of each joined connection when a guard
  loses its session. A connection checked out before the pass joins it.
- `assert_fence` takes the generation row `FOR SHARE`. Inside a write
  transaction it is a commit-order barrier against a bump.
- The scheduler pass holds a source guard. It drops that guard before the
  carry, so a wait on another pool cannot hold off a bump.

## Brainstorm

1. Run the carry inside the source pass, under its guard, and add a
   destination guard.
2. Drop the source guard, take both connections, then open one group guard
   for the source and the destination.
3. Assert the fence of each shard inside each carry transaction.
4. A tombstone on the destination for each clear, with the clear time.
5. A generation counter per DAG, shared by every shard.
6. A receipt on the destination: the version of each row that a copy
   delivered. It commits in the copy transaction.
7. Mark the source row as "in transfer" before the copy.
8. Never delete the source row. Let the owner ignore a stale copy.
9. Reverse the order: delete on the source first, then copy.

Decision: 2, 3 and 6.

- Option 1 opens a second guard while the pass holds one. A guard operation
  must take all of its slots at once, so this can stall.
- Option 2 follows the relay pattern and the issue text. Option 3 binds the
  barrier to the writing session too. That covers the known limit of a
  backend that the pass cannot end in time.
- Option 4 compares the time of two processes. Clock skew can then keep a
  newer row away or let an older row back.
- Option 5 needs a cross-shard counter. That is a new coordination point.
- Option 6 needs no clock. It tells "copied, then cleared" from "never
  copied" exactly, which is what the issue asks for.
- Option 7 cannot tell an operator clear from a lost copy.
- Option 8 keeps a stale row that a later reshard can carry back.
- Option 9 loses the override when the copy then fails.

## Reverse brainstorm: how could the fix break?

| Way to break it | Guard |
|---|---|
| A receipt stays on a shard after the row moves away. The same version comes back later. The carry skips it, and the override is lost. | The source delete transaction also deletes the receipt of the source. A TLA+ config proves the loss without it. |
| The carry waits for a pool connection while it holds a guard. A bump then waits too. | Both connections are checked out before the guard opens. |
| The carry opens a guard while it holds the source pass guard. | The pass guard drops first. The group guard opens after. |
| A lost guard leaves a statement running on the destination. | Both connections join the pass. Each transaction also asserts its fence. |
| The receipt and the row commit apart. A crash between them leaves a copy with no receipt. | One transaction writes both. |
| An operator writes on the new owner after the copy. The receipt then makes the carry skip a newer source write. | The receipt matches one version only. A newer source version is not skipped. The insert still yields to a row on the owner. |
| The receipt table grows without bound. | The key is `(kind, name)`. One row per DAG per shard. |
| A worker role has no grant on the new table. Every carry then fails. | The startup preflight checks the grants. |

## Six thinking hats

- **White (facts).** Two transactions on two databases. `updated_at` is a
  version tag, not a trusted time. The fence API has a group guard and an
  in-transaction assert.
- **Red (feel).** The clock-based tombstone feels fragile. A receipt feels
  like an idempotency key, which operators already understand.
- **Black (risk).** A new table needs a migration, grants and docs. A wrong
  receipt can lose an override. The round trip is the risky case, so the
  model checks it.
- **Yellow (value).** The carry becomes safe under DR, with the same
  barrier as every other fenced writer. A clear on the new owner stays
  across a retry, within the bound in "Limits".
- **Green (ideas).** Group the misplaced DAGs by owner, so one guard covers
  all DAGs that move to that owner.
- **Blue (process).** Spec first: a TLA+ model with a design config, three
  defect configs and a reachability witness. Then red integration tests for
  both acceptance criteria. Then the code. Then the gates.

## Design

### Fence (gap 1)

`carry_misplaced_overrides` groups the misplaced DAGs by owner. For each
owner it:

1. Checks out the destination connection. The source connection is the
   pass connection.
2. Opens `begin_fenced_groups` for the source and the owner. A fenced or
   held shard skips the group, and the next tick tries again.
3. Runs the carry of each DAG of the group under `run_fenced_pass`. Both
   connections join the pass first.

`carry_dag_override` takes the two shard ids. Each of its transactions
asserts the fence of its own shard first, then takes the registration lock.

### Receipt (gap 2)

A new table `harvest_schedule_override_receipts (kind, name,
row_updated_at, received_at)` lives on each shard.

The copy transaction on the destination:

1. Reads the receipt of the DAG.
2. When the receipt holds the `updated_at` of the source row, the copy
   already landed. It writes nothing.
3. Otherwise it inserts the row with `ON CONFLICT DO NOTHING`, and upserts
   the receipt with the `updated_at` of the source row.

The delete transaction on the source deletes the row that matches the
version, and the receipt of the source for that DAG.

## Model

`formal/tla/OverrideCarry.tla` models two or three shards, owner flips,
operator sets and clears, a copy, a delete that can fail, and a promotion.

| Config | Switch | Result |
|---|---|---|
| `OverrideCarry.cfg` | The design, two shards. | Pass. |
| `OverrideCarryThreeShards.cfg` | The design, three shards. | Pass. |
| `OverrideCarryNoReceipt.cfg` | No receipt. | Violates `ClearIsKept`. |
| `OverrideCarryKeepSourceReceipt.cfg` | The delete keeps the source receipt. | Violates `NothingLostWithoutAClear`. |
| `OverrideCarryUnfenced.cfg` | No fence check. | Violates `NoWriteAfterPromotion`. |
| `OverrideCarryFlipAfterFailedDelete.cfg` | A flip while a stale row stays. A known limit. | Violates `ClearIsKept`. |
| `OverrideCarryFlipMidCarry.cfg` | A flip between a copy and its delete. A known limit. | Violates `NothingLostWithoutAClear`. |
| `OverrideCarryReach.cfg` | The design. | Violates `NoClearOfACopiedRow`. |

`NothingLostWithoutAClear` asks that a quiet owner holds an override. It
does not ask for the last set version. A row that the owner already holds
wins over a copy, also when it is older. That rule predates this change.

## Tests

- AC 1: `a_promotion_during_a_carry_stops_every_write_of_the_old_process`.
  The copy blocks on the destination. A test promotes the destination and
  ends the guard sessions. It then frees the copy. Neither shard changes,
  and a later tick of the old process writes nothing.
- AC 2: `an_operator_clear_survives_a_retried_carry`. The source delete
  fails. The operator clears the override on the new owner. The next tick
  keeps it cleared while the delete still fails. The tick after that
  deletes the source row, and the override stays cleared.
- `a_promotion_of_the_source_during_a_carry_stops_its_delete`. The delete
  blocks on the source, and a test promotes the source. The source keeps
  its row.
- `a_fenced_carry_writes_nothing`. With no pass guard, each carry
  transaction refuses a write on a promoted shard.
- Regression: `an_override_survives_a_round_trip_reshard`. The override
  moves out, back and out again.
- `the_override_copy_records_are_covered_by_the_privilege_probe`. The
  preflight checks the grants of the new table.

## Limits

Issue #2602 closes the first three limits below and replaces "the owner row
wins". See `docs/design/DESIGN-2602.md`. The text below is the record of
this design.

The design holds when a routing change lands only while no carry runs and
no shard but the owner holds a row. Two configs show what breaks without
that. Both gaps predate this change. Issue #2602 tracks them.

- **A routing change after a failed delete.** The stale row stays on the
  source. When the routing returns the DAG to that shard, the stale row is
  live there. A clear there can come back from the shard that holds the
  copy, because the source holds no receipt for its own original row.
- **A routing change between a copy and its delete.** Two carries can then
  run in opposite directions. Each copy yields to the row that its
  destination holds, and both sources delete. The override is lost.
- **Three shards with two failed deletes.** One receipt per DAG holds one
  version. A second carry into a shard replaces the receipt of the first.
  This needs a routing change while a stale row stays, so the first limit
  covers it.
- **The owner row wins.** A copy never replaces a row on the owner, also
  when that row is older. An operator write on the source after the copy
  loses to the copy on the owner.

A fix of the first two needs more than a receipt per version: the review
of this change found that each extra mark exposes another interleaving.
The follow-up issue holds the counterexamples.
