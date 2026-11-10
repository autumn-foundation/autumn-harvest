## Host the durable agent under `HarvestPlugin` on SQLite (issue #2641)

An autumn-web app on SQLite now hosts the durable agent through
`HarvestPlugin`, with one call. It runs no `SqliteRuntime` of its own, so
it gets the management API, the Harvest UI, the schedules and the
retention of the plugin. The design is in `docs/design/DESIGN-2641.md`.

- `autumn_harvest_agent::register(builder, harness)` registers the agent
  workflows, their activities and the harness on a `HarvestBuilder`. It
  needs no backend feature. `postgres::register` now calls it.
- `HarvestPlugin::agent(harness)` applies it to the plugin. It works on
  both backends.
- The SQLite driver gives the plugin state to each `#[activity]` body, so
  each agent activity reads the harness there.
- `autumn_harvest_plugin::sqlite::agent` has `send`, `delete`,
  `current_run`, `state` and `decide` on the `HarvestSqlite` handle. They
  mirror the durable agent calls of `autumn_harvest_agent::sqlite`. `send`
  routes a steer or an interrupt by workflow id, in one driver command.
- `ActivityContext::keeps_run_tenant` is `false` on the embedded backend.
  The answer check then admits the tenant that the task declares, as the
  SQLite closure does.
- The SQLite build refuses `HarvestPlugin::agent` with
  `sqlite_per_tenant()`. The tenants would share one agent memory.

No new `WorkflowEvent` variant and no migration.

**Tests.** `autumn-harvest-plugin/tests/sqlite_plugin_agent.rs` runs the
overlay read suite of the agent crate on the plugin, as a third backend.
It covers ops, approvals, overlay reads, rollbacks, a policy ceiling and an
exact replay. It also starts an agent through
`HarvestSqlite::signal_with_start`, runs one model turn, and reads the
state through a query. Other tests cover an interrupt, retried keys and
the answer check tenant. `autumn-harvest-agent/tests/register.rs` checks
the neutral registration. CI runs the plugin suite in the `sqlite-plugin`
job.
