## The reshard override carry: safe under any routing change (issue #2602)

Follow-up to #2413. A routing change can now land at any time, also from a
process with an old router. The design is in `docs/design/DESIGN-2602.md`.

- **Serial carry.** `carry_dag_override` holds the registration lock of
  both shards from its read to the end of its delete. It takes them in
  ascending shard order. Two opposite carries then wait for each other, and
  the override stays on one shard (gap 2).
- **Floor.** Each row of `harvest_schedule_override_receipts` is now the
  floor of its DAG: the newest version that the shard cleared or replaced.
  A carry skips a version at or below the floor of its destination. So a
  clear stays after a route back (gap 1). One floor per DAG and shard holds
  for three shards too (gap 3).
- **Newest wins.** A copy replaces an older row on its destination and
  keeps a newer or an equal row. Before, the destination row always won, so
  a stale older row could shadow a newer set.
- **Version.** A set takes the later of the clock and one microsecond above
  the floor, in whole microseconds. A skewed clock cannot hide a new set.
- **Fence first.** Both carry transactions assert their fence before any
  lock or write. A fenced source now stops the copy too.
- **Model.** `formal/tla/OverrideCarry.tla` lets a routing change land at
  any time. `OverrideCarryFlipAfterFailedDelete.cfg` and
  `OverrideCarryFlipMidCarry.cfg` pass. Four new defect configs fail: no
  floor, a wall-clock version, the owner row wins, and no serial lock. The
  delivery-receipt configs `OverrideCarryNoReceipt.cfg` and
  `OverrideCarryKeepSourceReceipt.cfg` are gone.
- **Tests.** `schedule_override_reshard_db_tests`:
  `a_clear_on_the_old_shard_survives_a_route_back` (AC 2),
  `opposite_carries_keep_the_override` (AC 3),
  `a_newer_source_row_replaces_an_older_row_on_the_owner`,
  `a_replace_and_a_clear_raise_the_floor`,
  `a_set_takes_a_version_above_the_floor` and
  `the_floor_migration_keeps_only_retired_versions`.

### Upgrade notes for the next release guide

- **Migration.** `20261108045837_harvest_schedule_override_floor` changes no
  schema. It deletes each #2413 delivery receipt and keeps each receipt of a
  cleared or replaced version. No hot table, no `WorkflowEvent` variant, no
  replay impact.
- **Mixed versions.** An older process writes delivery receipts and drops
  the receipt of a source. Do not change shard routing until every process
  runs this release.
