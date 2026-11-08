# DESIGN-2602: Keep the reshard override carry safe under any routing change

Issue #2602. Parent epic #2384. Follow-up to #2413 (PR #2600).

## Problem

The #2413 design holds only when a routing change lands while no carry runs
and no shard but the owner holds a row. Without that bound, three gaps open.

1. **A route back after a failed delete undoes a clear.** Shard A holds v1
   and carries it to B. The delete on A fails. The route returns to A, and
   the operator clears v1 on A. B then carries v1 back, because A holds no
   receipt for its own row.
2. **Two opposite carries lose the override.** A copies v1 to B. Before A
   deletes, B carries v1 back to A under the next route. Each copy yields
   to the row on its destination, and both sources delete.
3. **Three shards overwrite one receipt.** A second carry into a shard
   replaces the receipt of the first. A clear there can come back.

## Facts

- The copy and the delete are two transactions on two databases.
- Each override writer takes the registration lock of its shard
  (`pg_advisory_xact_lock`). The carry takes it in each transaction.
- The `updated_at` of a row is its version. A carry keeps it, and the
  delete matches it.
- `updated_at` comes from the clock of the writing process. Two processes
  can disagree on the time.
- Two processes can run two routers during a rolling routing change. So a
  "routing change" can happen inside one carry.
- Postgres does not see a lock cycle that runs through a client with two
  connections. Only `lock_timeout` ends it.

## Brainstorm

1. Make a routing change wait until every carry settles: a router
   generation, and a carry runs only under the generation that it read.
2. Order versions across shards, so a copy tells a newer row from an older
   one.
3. Lock both shards for the whole carry, so no other carry of either shard
   runs between its copy and its delete.
4. Record each version that an operator clears or replaces, on its shard
   (a tombstone).
5. One tombstone per version. Prune old tombstones by age.
6. One floor per DAG and shard: the newest version that the shard retired.
   A copy at or below the floor writes nothing.
7. Keep the #2413 delivery receipt beside the tombstones.
8. A hybrid logical clock for `updated_at`: a set takes the larger of the
   clock and one microsecond above each version that its shard knows.
9. A lease per DAG on the default shard.
10. Copy the source tombstones with the row.

Decision: 3, 6 and 8, and a newer copy replaces an older row (2).

- Option 1 needs a new shared coordination point. A process with an old
  router does not read it. It also contradicts acceptance criterion 1.
- Option 9 needs a fencing token on a third database. A lease that expires
  in a pause still lets a late delete commit.
- Option 3 uses the registration lock that every writer already takes. It
  closes gap 2: the second carry cannot start until the first ends.
- Options 4 and 5 close gaps 1 and 3, but the table grows. A pruned
  tombstone opens gap 1 again.
- Option 6 holds one row per DAG and shard, so the table stays small. It
  needs an order on versions, so it needs option 8.
- Option 8 keeps `updated_at` close to the wall clock. A skewed clock
  cannot give a version at or below the floor.
- Option 7 is wrong under any routing change. TLC finds a run: a copy
  yields to a newer row and still records its version. A later real copy
  of that version is then skipped, and the override is lost.
- Option 10 is not needed. The floor and the order cover it.
- "The owner row wins" (#2413) is wrong under any routing change. A stale
  older row can shadow a newer row. A floor then removes the stale row, and
  the override is lost. So a newer copy replaces an older row.

## Reverse brainstorm: how could the fix break?

| Way to break it | Guard |
|---|---|
| Two opposite carries take the two locks in opposite order. Each waits for the other until `lock_timeout`. | Each carry takes the locks in ascending shard order. |
| A set gets a version at or below the floor. Its round trip is then skipped. | The set takes one microsecond above the floor and the row. `OverrideCarryWallClock.cfg` shows the loss without it. |
| A copy raises the floor when it lands on an empty shard. A round trip is then skipped. | A copy raises the floor only when it replaces a row. |
| A legacy delivery receipt becomes a floor. A round trip is then skipped. | The migration deletes each receipt at or above the live row of its shard. |
| The source delete drops the floor of the source. A cleared version comes back. | The delete keeps the floor. |
| The carry holds the source lock while it waits on the destination. A bump waits too. | Each wait is at most 1 s. The DR guards hold off a bump only for that time. |
| A copy compares versions with nanoseconds. Postgres stores microseconds. | Each version is truncated to microseconds before a compare or a write. |
| A newer copy replaces a row on the owner that an operator set later in real time. | Within the clock skew between two processes, that is the order of the two writes. Each write takes a version above all that its shard knows. |

## Six thinking hats

- **White (facts).** Two databases, two transactions per carry. The
  registration lock serializes each writer of one shard. `updated_at` is a
  version and a near-wall-clock time.
- **Red (feel).** More marks feel fragile: the #2600 review tried four,
  and each opened a new run. A floor with an order feels like a known
  pattern: a last-writer-wins register with a tombstone.
- **Black (risk).** The carry now holds one lock across two databases. A
  wrong order gives a cross-database deadlock that only a timeout ends.
  The table changes meaning, so legacy rows need a migration.
- **Yellow (value).** A routing change can land at any time, also from a
  process with an old router. A clear stays. The newest set wins. The table
  stays at one row per DAG and shard.
- **Green (ideas).** Reuse the receipt table as the floor table. Keep the
  table name, so the change is an expand migration.
- **Blue (process).** Spec first: change the TLA+ model and run TLC for the
  design and for each defect. Then red DB tests for gaps 1 and 2, and for
  each new rule. Then the code. Then the gates and a review.

## Design

### Serial carry (gap 2)

`carry_dag_override` opens a transaction on the source, then one on the
destination. Each bounds its lock waits to 1 s and asserts its fence. It
then takes the registration lock of both shards in ascending shard order.
It reads the source row under those locks. The destination transaction
commits the copy first. The source transaction then deletes the row and
commits. So no other carry of either shard runs between the copy and the
delete.

### Floor (gaps 1 and 3)

`harvest_schedule_override_receipts` keeps its name and key. Its
`row_updated_at` is now the floor: the newest version that the shard
retired for the DAG.

- `update_dag_override` raises the floor to the version of the row that it
  replaces or clears.
- `clear_dag_override` raises the floor to the version of the row that it
  deletes.
- The copy writes nothing when the source version is at or below the floor
  of the destination.

### Version order

- A set takes `max(now, floor + 1 µs, row + 1 µs)`, truncated to
  microseconds.
- A copy into an empty shard inserts the row.
- A copy replaces an older row, and that row raises the floor.
- A copy keeps a newer or an equal row.

### Migration

A new migration deletes each receipt at or above the live row of its shard.
That receipt is a delivery receipt of #2413: its row is still there, or it
yielded to an older row. Each other receipt is a version that the shard
cleared or replaced, so it is a valid floor.

## Model

`formal/tla/OverrideCarry.tla` lets a routing change land at any time.

| Config | Switch | Result |
|---|---|---|
| `OverrideCarry.cfg` | The design, two shards. | Pass. |
| `OverrideCarryThreeShards.cfg` | Three shards. | Pass. |
| `OverrideCarryFlipAfterFailedDelete.cfg` | `FlipWhen = "idle"`. | Pass. |
| `OverrideCarryFlipMidCarry.cfg` | `FlipWhen = "any"`, a deeper bound. | Pass. |
| `OverrideCarryNoFloor.cfg` | `Floor = FALSE` | Violates `ClearIsKept`. |
| `OverrideCarryWallClock.cfg` | `Hlc = FALSE` | Violates `ClearIsKept`. |
| `OverrideCarryOwnerRowWins.cfg` | `Newest = FALSE` | Violates `NothingLostWithoutAClear`. |
| `OverrideCarryUnserialized.cfg` | `Serial = FALSE` | Violates `NothingLostWithoutAClear`. |
| `OverrideCarryUnfenced.cfg` | `Fenced = FALSE` | Violates `NoWriteAfterPromotion`. |
| `OverrideCarryReach.cfg` | The design. | Violates `NoClearOfACopiedRow`. |

`ClearIsKept` now also covers a replaced version. An operator who replaces
v1 with v2 and then clears v2 must not see v1 come back.

The #2413 configs `OverrideCarryNoReceipt.cfg` and
`OverrideCarryKeepSourceReceipt.cfg` model the delivery receipt, which is
gone. `OverrideCarryNoFloor.cfg` replaces them.

## Tests

- AC 2: `a_clear_on_the_old_shard_survives_a_route_back`. Gap 1 through the
  scheduler: a failed delete, a route back, a clear on the old shard.
- AC 3: `opposite_carries_keep_the_override`. Gap 2: the delete of one
  carry waits after its copy, and the opposite carry runs.
- `a_newer_source_row_replaces_an_older_row_on_the_owner`.
- `a_set_takes_a_version_above_the_floor`.
- `the_floor_migration_keeps_only_retired_versions`.

## Limits

- **Concurrent sets.** Two operators who set the same DAG on two shards
  under two routers write concurrently. The newer version wins. Clock skew
  can order two writes that are closer than the skew.
- **Two shards of one pool** share one table, so nothing moves between
  them. That rule is unchanged (#2405).
