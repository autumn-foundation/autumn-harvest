## The reshard override carry: fence both shards, keep a clear across a retry (issue #2413)

Two fixes to the carry of a DAG schedule override to its new shard
(#2389). The design is in `docs/design/DESIGN-2413.md`.

- **DR fence.** `carry_misplaced_overrides` groups the misplaced DAGs by
  owner. For each owner it checks out the destination connection first,
  then opens one `begin_fenced_groups` guard for the source and the owner.
  The carry runs under `run_fenced_pass`, and both connections join the
  pass. A fenced or held shard skips the group until the next tick. A
  stopped pass ends the remaining groups of that tick.
- **In-transaction fence.** `carry_dag_override` now takes the source and
  the destination shard. Each of its two transactions bounds its lock waits
  to 1 s, asserts the fence of its own shard, then takes the registration
  lock.
- **Copy receipts.** A new table, `harvest_schedule_override_receipts`,
  records the `updated_at` of each row that a copy delivered. The copy
  writes the row and the receipt in one transaction. A retry that finds a
  receipt for the same version writes nothing. So a clear on the new owner
  stays after a failed source delete. The source delete also removes the
  receipt of the source, so a round trip reshard still delivers the row.
- **Limits.** The design holds when a routing change lands only while no
  carry runs and no shard but the owner holds a row. Issue #2602 tracks a
  routing change after a failed delete or between a copy and its delete.
- **Model.** `formal/tla/OverrideCarry.tla` checks the design with two and
  three shards. Three defect configs fail: no receipt, a kept source
  receipt and no fence. Two configs witness the limits.
- **Tests.** `schedule_override_reshard_db_tests`:
  `a_promotion_during_a_carry_stops_every_write_of_the_old_process` (AC 1),
  `an_operator_clear_survives_a_retried_carry` (AC 2),
  `a_promotion_of_the_source_during_a_carry_stops_its_delete`,
  `a_fenced_carry_writes_nothing` and
  `an_override_survives_a_round_trip_reshard`. The plugin adds
  `the_override_copy_records_are_covered_by_the_privilege_probe`.

### Upgrade notes for the next release guide

- **Migration.** `20261106145618_harvest_schedule_override_receipts`
  creates one new table. It takes no lock on a hot table. No existing table
  changes, no `WorkflowEvent` variant, no replay impact.
- **Grants.** Each worker role needs `SELECT`, `INSERT`, `UPDATE` and
  `DELETE` on `harvest_schedule_override_receipts`. `GET /admin/preflight`
  checks them. A missing grant fails each carry. Until the grant is in
  place, the new owner can run the code value, and each tick retries.
- **Mixed versions.** An older process carries without a receipt and
  leaves the receipt of its source. Do not change shard routing until every
  process runs this release. On a rollback that keeps the table, truncate it
  before you upgrade again.
- **Breaking.** `schedule_override::carry_dag_override` takes `from_shard`
  and `to_shard` after `to`. It can return `HarvestError::ShardFenced`.
