## Plugin — SQLite app: schedules, retention, MCP, webhooks and connectors (issue #2471, epic #2466)

A `HarvestPlugin` app on `autumn-web/sqlite` now builds with workflow
schedules, `retention`, `mcp_tools`, `mcp_tasks`, `webhooks` and
`connector`. Slice 1 refused each one at build. The Postgres default build
does not change. The design record is `docs/design/DESIGN-2471.md`.

- **Schedules.** `HarvestPlugin::workflow_schedule` adds a code schedule on
  both backends. The SQLite driver upserts it at boot.
  `harvest.scheduler_enabled = false` fires no slot and upserts no code
  schedule, as on Postgres. The engine gains
  `SqliteRuntime::set_schedules_enabled` and `trigger_schedule`, and
  `SqliteReader::list_schedules`.
- **Retention.** `RetentionConfig` maps to the new
  `autumn_harvest_sqlite::PurgePolicy`: the global age, the per-type
  `overrides` and `dry_run`. The driver runs a pass every
  `tick_interval_secs`, between drive passes. `tenant_overrides`, `summary`
  and `payload_orphan_grace_secs` are refused at build.
- **Admin routes.** `GET`/`POST`/`PATCH`/`DELETE` of `/admin/schedules`,
  `/pause`, `/resume`, `/trigger`, `GET /admin/retention` and
  `POST /admin/retention/run-now` serve on SQLite, with the Postgres paths
  and field names. A schedule `{id}` is a UUID v5 of the workflow name. Each
  write adds one audit row. Backfill, runs, decisions and preview answer
  `501`.
- **One seam.** `HarvestApiState::sqlite_handle` is `None` on Postgres and
  the control Harvest on SQLite. MCP tools and tasks, webhooks and
  connectors branch there. A request before startup is a `503`, never the
  Postgres path.
- **Connectors.** A poison message goes to `harvest_connector_dead_letters`
  in the Harvest file, through `SqliteDeadLetterSink`. A `queue` and a
  `Starts` binding in `BrokerCoordinates` mode are refused at build.
- **Webhooks.** A `queue` is refused at build. Each dispatch writes its
  audit row to the Harvest file.
- **Refactors.** `validate_connectors` and `build_mcp_routes` moved out of
  `build_postgres`, so both backends run one copy.
- **No new `WorkflowEvent` variant, no Postgres migration.** The SQLite
  auth store adds one table, `harvest_connector_dead_letters`.
- **Tests.** `autumn-harvest-sqlite`: `schedule_admin` and
  `retention_policy`. Plugin: `ops_parity` and `trigger_parity` in
  `tests/sqlite_plugin.rs`. CI's `sqlite-plugin` job turns on `mcp`,
  `webhooks` and `connectors`.
