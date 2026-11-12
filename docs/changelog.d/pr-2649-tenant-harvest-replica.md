## The tenant Harvest file ships with its database (issue #2649, PR #2659)

`HarvestPlugin::sqlite_per_tenant()` now ships each `<tenant>.harvest.db`
with its tenant database when the fleet replicates. A takeover restores the
runs of the tenant, not only its data.

- **Layout.** The file ships under `<tenant prefix>/harvest/`, the layout of
  autumn ADR 0040, with the autumn `Replicator` and the ownership epoch. The
  fleet's tombstone marker and purge cover both files.
- **Restore.** `on_open` restores a missing file, under `restore_missing`,
  before the driver starts. The open also replaces a file from an older
  epoch of another owner. The Harvest file records the identity of its
  tenant database file, so it follows a restore of the database.
  `HarvestFleet::restore(key, target)` keeps its target on disk and
  restores both files to one point in time.
- **Fence.** A host that lost the range ships nothing, at a tick or at a
  close.
- **Config.** The destination comes from `[replication]`, or from
  `HarvestPlugin::sqlite_tenant_replica`.
- **SQLite seams.** `SqliteRuntime::open_replicated` turns the auto
  checkpoint off. `SqliteRuntime::raise_fleet_epoch` and
  `fleet_epoch::read` keep the epoch record. `FileWriterLock` holds the
  writer lock without a runtime.
- **Invariants.** No `WorkflowEvent` variant and no migration. The epoch
  table `harvest_fleet_epoch` is outside the Harvest schema. `harvest_events`
  is not rewritten: a restore replaces the whole file.
- **Tests.** `tests/sqlite_plugin_fleet_replica.rs` runs two hosts on one
  destination: a takeover mid-run, a fenced old owner, a zombie owner, a
  failback, a re-created tenant, a purge, and restores at the latest state
  and at a point in time. See `docs/design/DESIGN-2649.md`.
