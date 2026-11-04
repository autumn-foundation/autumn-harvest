# DESIGN-2471: schedules, retention, MCP, webhooks and connectors on the SQLite plugin

Issue #2471. Parent epic #2466. Follows #2467.

## Problem

Slice 1 refuses these options at build on SQLite and names this issue:
workflow schedules, `retention`, `mcp_tools`, `mcp_tasks`, `webhooks` and
`connector`. The issue asks for these items:

1. Honour `harvest.scheduler_enabled` and `HarvestPlugin::retention(..)`
   with `SqliteRuntime::upsert_schedule` and `purge_terminal`.
2. Serve the schedule and retention admin routes.
3. Start the runs of MCP tools and tasks, webhook receivers and broker
   connectors through the `HarvestSqlite` handle, not the Postgres start
   path.

## Facts

- `autumn-harvest-sqlite` already stores schedules (#2031), overrides
  (#2390) and a purge of terminal runs. `poll_once` fires due slots.
- The engine has no switch for the scheduler and no manual trigger. Its
  purge takes one global age. Postgres also has per-type ages and a dry run.
- On the plugin, a workflow schedule comes only from a DAG or the canary.
  Both are refused on SQLite. So no app can declare a schedule in code.
- MCP tools, MCP tasks, webhooks and connectors share no starter trait.
  Each one calls a Postgres axum handler of `crate::api` through
  `HarvestApiState`. On SQLite that state has no runtime and no pool.
- The SQLite start and signal-with-start handlers already answer the
  Postgres JSON shape. The callers read that shape back.
- A connector `Starts` binding in `BrokerCoordinates` mode needs the
  Postgres start-idempotency table (#808). SQLite has none.
- A webhook or connector `queue` has no SQLite meaning. One writer drains
  every queue.
- The connector dead-letter sink is hard-wired to Postgres.

## Brainstorm

1. Add a Postgres branch and a SQLite branch inside each `crate::api`
   handler.
2. Write a separate SQLite copy of each surface.
3. Add a `RunStarter` trait object to `HarvestApiState`.
4. Add a typed `HarvestSqlite` slot to `HarvestApiState`. Branch only at
   the I/O seam of each surface: the start, the status read, the signal,
   the update, the cancel. Keep the pure code shared.
5. Run retention as a driver command on its own interval.
6. Run retention from a second task with its own connection.
7. Key a SQLite schedule by a UUID v5 of its workflow name, so the
   Postgres `{id}` routes work unchanged.
8. Add `HarvestPlugin::workflow_schedule(..)`, so an app can declare a
   schedule in code on both backends.

## Reverse brainstorm: how could this fail?

- A request reaches the Postgres path on a SQLite app. It answers a
  confusing `400` such as "runtime is not started". A connector reads that
  `400` as transient and retries for ever. Option 4 sends every request on
  a SQLite app to SQLite, and answers `503` before startup.
- A purge removes runs that an archiver should keep. The plugin cannot set
  an archiver, and the SQLite path has none. A tenant age or a summary
  policy has no SQLite store. So the build refuses them.
- A second writer task deletes rows while the driver writes. Option 6
  does this. Option 5 keeps one writer.
- A connector `Starts` binding in `BrokerCoordinates` mode dedupes on a
  table that SQLite does not have. A redelivery starts a second run. The
  build refuses it.
- A webhook or connector `queue` is silently dropped. The build refuses it.
- A poison message cannot be dead-lettered. The runtime never acks it, so
  the binding stalls. A SQLite sink writes the record to the Harvest file.
- A schedule fires while `scheduler_enabled = false`. The driver turns the
  engine scheduler off and upserts no code schedule.
- A schedule id from Postgres is a random UUID. A SQLite client that keeps
  one would break on a rename. Option 7 makes the id stable for the name.
- `skip` does not count a triggered run, so a trigger and a slot overlap.
  The trigger marks its run with the schedule name, so `skip` counts it.

## Six hats

- **White (facts).** Six refusals. Four call sites start runs:
  `mcp_tools::start_tool`, `webhook_receiver::handle_webhook`,
  `connector::dispatch` and `mcp_tasks::tools_call`. MCP status, signal,
  update, watch and tasks read Postgres rows.
- **Red (feel).** A copy of each surface is the fastest. It also drifts,
  as #2469 found for the auth layers.
- **Black (risk).** A silent fallback to Postgres. A purge that removes too
  much. A double start on a redelivery. A stalled poison message.
- **Yellow (value).** Small apps and test apps on SQLite get schedules,
  retention, MCP, webhooks and broker triggers with the same code.
- **Green (ideas).** One typed slot. The pure helpers stay shared: MCP
  descriptors, task snapshots, webhook mapping and the connector
  disposition rules.
- **Blue (process).** Engine tests first (red), then the engine (green).
  Then the plugin tests (red), the plugin (green), and a refactor and a
  review.

## Decision

- **Engine.**
  - `SqliteRuntime::set_schedules_enabled(false)` fires no slot.
  - `trigger_schedule(name, force, overlap)` starts one run now. It honours
    `skip`, keeps the slot, and marks the run with the schedule name.
  - `purge_with(&PurgePolicy)` applies a global age, per-type ages and a
    dry run. It returns a per-type count.
  - `SqliteReader::list_schedules` reads schedules off the driver.
- **Schedules.**
  - `HarvestPlugin::workflow_schedule(..)` adds a code schedule on both
    backends.
  - The SQLite driver upserts each code schedule at open, unless
    `harvest.scheduler_enabled = false`. Then it fires no slot.
- **Retention.** `RetentionConfig` maps to a `PurgePolicy`. The driver runs
  a pass every `tick_interval_secs`, between drive passes.
  `tenant_overrides`, `summary` and `payload_orphan_grace_secs` are refused
  at build.
- **Admin routes.** These routes serve on SQLite, with the Postgres paths
  and field names:
  - `GET /admin/schedules` and `GET /admin/schedules/{id}`.
  - `POST /admin/schedules/workflow`.
  - `PATCH /admin/schedules/{id}`, which sets an override on a code
    schedule and edits an API schedule in place.
  - `POST /admin/schedules/{id}/pause`, `/resume` and `/trigger`.
  - `DELETE /admin/schedules/{id}`.
  - `GET /admin/retention` and `POST /admin/retention/run-now`.

  `{id}` is the UUID v5 of the workflow name. Backfill, runs, decisions and
  preview answer `501`.
- **One seam.** `HarvestApiState::sqlite_handle()` is `None` on Postgres.
  On SQLite it is the control Harvest, or a `503` before startup. Each
  surface branches there.
- **Webhooks.** A delivery starts or signals through the handle and answers
  the Postgres envelope. The audit row goes through `AuthStore`. A trigger
  with a `queue` is refused at build.
- **Connectors.** `dispatch` calls the handle. The dead-letter sink writes
  to `harvest_connector_dead_letters` in the Harvest file. A `queue` and a
  `Starts` binding in `BrokerCoordinates` mode are refused at build.
- **MCP tools and tasks.** They are mounted on SQLite with the same
  descriptors and auth layers. Each tool and task call reads and writes
  through the handle. `watch` polls the reader, because SQLite has no
  `LISTEN`.

## Out of scope

- The synthetic canary and the agent overlay store. They stay refused and
  now name the epic.
- Schedule backfill, run listing, decisions and preview.
- Per-tenant schedules from code. A tenant driver runs retention and fires
  its stored schedules. Only the control Harvest gets the code schedules.

## Acceptance

1. A SQLite app with a code schedule fires it, and does not fire it with
   `harvest.scheduler_enabled = false`.
2. `HarvestPlugin::retention(..)` purges old terminal runs on SQLite.
3. The schedule and retention admin routes serve on SQLite.
4. An MCP tool, an MCP task, a webhook and a connector each start a run on
   SQLite through the handle.
5. The Postgres build does not change. Its suites stay green.
