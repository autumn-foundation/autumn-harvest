## The reshard override carry: fence both shards, keep a clear across a retry (issue #2413)

Two fixes to the carry of a DAG schedule override to its new shard
(#2389). The design is in `docs/design/DESIGN-2413.md`.

- **DR fence.** `carry_misplaced_overrides` groups the misplaced DAGs by
  owner. For each owner it checks out the destination connection first,
  then opens one `begin_fenced_groups` guard for the source and the owner.
  The carry runs under `run_fenced_pass`, and both connections join the
  pass. A fenced or held shard skips the group until the next tick.
- **In-transaction fence.** `carry_dag_override` now takes the source and
  the destination shard. Each of its two transactions asserts the fence of
  its own shard first, then takes the registration lock.
- **Copy receipts.** A new table, `harvest_schedule_override_receipts`,
  records the `updated_at` of each row that a copy delivered. The copy
  writes the row and the receipt in one transaction. A retry that finds a
  receipt for the same version writes nothing. So a clear on the new owner
  stays after a failed source delete. The source delete also removes the
  receipt of the source, so a round trip reshard still delivers the row.
- **Migration.** `20261106145618_harvest_schedule_override_receipts`
  creates the table. It takes no lock on a hot table. Each worker role needs
  `SELECT`, `INSERT`, `UPDATE` and `DELETE` on it. The startup preflight
  checks the grants. No `WorkflowEvent` variant, no replay impact.
- **Breaking.** `schedule_override::carry_dag_override` takes two more
  arguments, `from_shard` and `to_shard`.
- **Model.** `formal/tla/OverrideCarry.tla` checks the design. Three defect
  configs fail: no receipt, a kept source receipt and no fence.
- **Tests.** `schedule_override_reshard_db_tests`:
  `a_promotion_during_a_carry_stops_every_write_of_the_old_process` (AC 1),
  `an_operator_clear_survives_a_retried_carry` (AC 2),
  `an_override_survives_a_round_trip_reshard` and
  `a_fenced_carry_writes_nothing`.
