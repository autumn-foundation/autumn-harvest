## Host the durable agent under `HarvestPlugin` on SQLite (issue #2641)

An autumn-web app on SQLite now hosts the durable agent through
`HarvestPlugin`. It runs no `SqliteRuntime` of its own, so it gets the
management API, the Harvest UI, the schedules and the retention of the
plugin. The design is in `docs/design/DESIGN-2641.md`.

- `autumn_harvest_agent::register(builder, harness)` registers the agent
  workflows, their activities and the harness on a `HarvestBuilder`. It
  needs no backend feature. `postgres::register` now calls it.
- `HarvestPlugin::agent(harness)` applies it to the plugin. It works on
  both backends.
- The SQLite driver needs no change. It gives the plugin state to each
  `#[activity]` body, so each agent activity reads the harness there.
- `autumn_harvest_plugin::sqlite::agent` has `send`, `delete`,
  `current_run`, `state` and `decide` on the `HarvestSqlite` handle. They
  mirror `autumn_harvest_agent::sqlite`. `send` signals a steer or an
  interrupt to the running run first.

No new `WorkflowEvent` variant and no migration.

**Tests.** `autumn-harvest-plugin/tests/sqlite_plugin_agent.rs` runs the
overlay read suite of the agent crate on the plugin, as a third backend:
ops, approvals, overlay reads, a rollback, a policy ceiling and an exact
replay. It also starts an agent through `HarvestSqlite::signal_with_start`,
runs one model turn, reads the state through a query, and ends an approval
wait with an interrupt. `autumn-harvest-agent/tests/register.rs` checks the
neutral registration. CI runs the plugin suite in the `sqlite-plugin` job.
