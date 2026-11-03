## Plugin — SQLite app: split mode and per-tenant Harvest (issue #2472, epic #2466)

Two topologies for a `HarvestPlugin` app on `autumn-web/sqlite`. The
Postgres default build does not change. The design record is
`docs/design/DESIGN-2472.md`.

- **Split and external mode.** `harvest.mode = "split"` or `"external"` no
  longer fails the boot of a SQLite app. The plugin takes its Postgres path
  on `harvest.database.url`, with the whole management API and every
  builder option. Under `sqlite` it builds that Postgres pool itself, with
  `pg_tls::connect`, because `autumn-web` builds SQLite pools only. Its
  `sslmode=require` checks the certificate, and it reads no `sslrootcert`.
  The boot probes one connection and logs the remedy. The
  outbox migration set is not registered on the SQLite app database. The
  outbox relay and `AppDbPool` stay off. The build refuses
  `harvest.sqlite.path` and `sqlite_per_tenant()` in these modes.
- **Per-tenant Harvest.** `HarvestPlugin::sqlite_per_tenant()` gives each
  open fleet database its own Harvest file (`acme.db` gives
  `acme.harvest.db`) and its own driver. A `FleetLifecycle` starts the
  driver on open and registers it as a worker of the database, so a close
  stops it. A delete removes the Harvest file and its sidecars. The control
  Harvest keeps running. `HarvestFleet` (extension and extractor) returns a
  `HarvestSqlite` that holds the tenant's lease. `HarvestTenant::current()`
  gives the tenant key inside an activity. `.api(path)` serves the core
  routes on one tenant under `{path}/tenants/{key}/..`, behind the same auth
  and #1802 gate. These routes never create a tenant. The boot fails
  without a fleet. In `slot` mode the slot database is the unit.
- **No new `WorkflowEvent` variant. No migration.** autumn#3536 is not
  needed: Harvest applies its own schema to its own file.
- **Limits.** A closed tenant runs nothing until it opens again. A fleet
  restore does not roll back the Harvest file. Replication does not ship it.

Tests: `tests/sqlite_plugin_split.rs` runs a workflow with an activity and a
signal in split and external mode on a Postgres container (new CI job
`sqlite-plugin-split`). `tests/sqlite_plugin_fleet.rs` covers per-tenant
files and drivers, isolation, the tenant routes, a close and reopen, a
delete with its sidecars, the lease count, a tenant open before startup,
the mutation gate, a route that never creates a tenant and a slot fleet.
The split suite also checks the build refusals. Unit tests cover the mode
routing, the refusals, the pool builder, the outbox predicate and the
registry generations.
