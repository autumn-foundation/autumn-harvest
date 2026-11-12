## A closed tenant wakes when its next timer or schedule is due (issue #2650)

`HarvestPlugin::sqlite_per_tenant()` now opens a closed tenant when its
Harvest file has due work. Before, a durable timer or a schedule slot of a
closed tenant fired only when a request opened the tenant again.

- `SqliteRuntime::next_wake_at()` returns the earliest due time, in epoch
  milliseconds: an unfired timer, an unpaused schedule slot, an activity
  retry backoff, a run backoff or a run deadline. Each source counts only
  for a running run. A disabled scheduler gives no slot. A backoff counts
  only while it is in the future. The read starts from the running runs
  and reads timers and tasks by run key.
- Each tenant driver records the value at each idle point when it changes,
  and when the tenant closes. A driver that stops with work in flight
  records now. A deadline that a drive cannot clear comes back once a
  minute, so a broken run never makes the sweep open its tenant on each
  tick.
- The values go to `harvest_tenant_wakes (tenant_key, next_wake_at)` in the
  control database. The driver never waits for that write while it runs.
  `TENANT_WAKES_SCHEMA_SQL` holds the DDL.
- A sweep on `harvest.sqlite.tick_ms` opens each due tenant that this host
  owns and that has no driver. It uses a background open, runs the driver
  until it is idle, then lets the fleet close the tenant. A host never
  opens or removes the row of a tenant that another host owns.
- A delete removes the row. A restore records now, so the next sweep
  applies it.
- No new `WorkflowEvent` variant. No Harvest schema change. The control
  database gets one new table, created at boot.
- Tests: `tests/integration/next_wake_at.rs` in `autumn-harvest-sqlite`
  covers timers, schedules, a paused schedule, a disabled scheduler, no
  deadline, an activity retry and a run deadline.
  `tests/sqlite_plugin_fleet_wake.rs` proves that a closed tenant fires a
  5 s timer within one sweep of its deadline, that a slot of a closed
  tenant runs once, and that a second host that does not own the tenant
  never opens it. The design record is `docs/design/DESIGN-2650.md`.
