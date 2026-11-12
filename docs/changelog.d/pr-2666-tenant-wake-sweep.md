## A closed tenant wakes when its next timer or schedule is due (issue #2650, PR #2666)

`HarvestPlugin::sqlite_per_tenant()` now opens a closed tenant when its
Harvest file has due work. Before, a durable timer or a schedule slot of a
closed tenant fired only when a request opened the tenant again.

- `SqliteRuntime::next_wake_at()` returns the earliest due time, in epoch
  milliseconds: an unfired timer, an unpaused schedule slot, an activity
  retry backoff, a run backoff or a run deadline. Each source counts only
  for a running run. A disabled scheduler gives no slot. A backoff counts
  until a drive starts its run. `next_wake_since(consumed)` keeps a
  backoff that ended after the last drive, so a tenant that closes between
  two drives keeps it. `next_wake_after(after)` skips the deadlines up to a
  bound. The read finds the running runs through the
  state index, and reads timers and tasks by run key.
- Each tenant driver records in a memory book: now when work starts, the
  value at each idle point when it changes, and the final value at the
  stop. A driver never waits for the control database. A deadline that a
  drive cannot clear comes back once a minute, and never hides a healthy
  deadline of the same tenant.
- The sweep writes the book to `harvest_tenant_wakes (tenant_key,
  routing_slot, next_wake_at)` in the control database, in one tracked
  immediate transaction. A host writes only the rows of the slots that it
  owns. `TENANT_WAKES_SCHEMA_SQL` holds the DDL.
- The sweep runs on `harvest.sqlite.tick_ms`. It reads the due rows of the
  owned slot ranges with a cursor, and opens each closed tenant in the
  background. It runs the driver until it is idle, then lets the fleet
  close the tenant. A failed wake tries again a minute later. Only a fleet
  delete removes a row. A restore records now.
- No new `WorkflowEvent` variant. No Harvest schema change. The control
  database gets one new table, created at boot.
- Tests: `tests/integration/next_wake_at.rs` in `autumn-harvest-sqlite`
  covers timers, schedules, a paused schedule, a disabled scheduler, no
  deadline, a terminated run, an activity retry, a past run backoff, a run
  deadline and `next_wake_after`. `tests/sqlite_plugin_fleet_wake.rs`
  shows that a closed tenant fires a 5 s timer within one sweep of its
  deadline, that a slot of a closed tenant runs once, that a second host on
  the same volume never opens the tenant or changes a row it does not own,
  and that a delete removes the row. Unit tests cover the book, the paged
  owned-range read, a dropped flush, the stuck rule and each driver record.
  The design record is `docs/design/DESIGN-2650.md`.
