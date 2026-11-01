## Plugin — `HarvestPlugin` on SQLite, slice 1 (issue #2467, epic #2466)

`autumn-harvest-plugin` gains a `sqlite` feature. It turns on
`autumn-web/sqlite` and `autumn-harvest-sqlite`, so `HarvestPlugin` builds and
boots in an Autumn app on the SQLite lane, including a SQLite fleet app. Like
`autumn-web/sqlite`, only an end application may enable it. The Postgres
default build does not change.

- **Storage.** Harvest opens its own file next to the app database:
  `app.db` gives `app.harvest.db`. In a fleet app that is the control
  database. `HarvestPlugin::sqlite_path`, then `[harvest.sqlite] path` /
  `AUTUMN_HARVEST_SQLITE__PATH` override it. No Postgres migration set is registered on a SQLite app.
- **Driver.** One task on the app's runtime owns the `SqliteRuntime` and drives it after each
  command and on `[harvest.sqlite] tick_ms` (default 250). Reads use a
  read-only `SqliteReader`. A query replays on the caller's task.
- **App state.** `HarvestPlugin::state(..)` values and `AppState` reach
  `ctx.state::<T>()` in workflows, activities, local activities and query
  replays, as on Postgres. `SqliteRuntime::set_shared_state` is new.
- **Handle.** `autumn_harvest_plugin::sqlite::HarvestSqlite` is an `AppState`
  extension and an axum extractor: start, signal-with-start, signal (by run
  or workflow id), cancel, terminate, query, update, describe, list,
  history, and `shutdown`, which releases the writer lock. A start checks
  the workflow's `input_schema`, and a signal checks its `#[signal]`
  `arg_schema`, as on Postgres.
- **Management API.** The core routes keep their Postgres paths and field
  names. Every other route answers `501` and links the epic. An unsupported
  start option is a `400` that names it. The #1802 mutation gate and
  `api_with_auth` work as on Postgres.
- **Loud refusals.** Each builder option or mode with no SQLite port fails
  the build or the boot and names its issue (#2468 to #2472).

Supporting changes: `HarvestBuilder::take_state` and `take_activities`, and
`SqliteReader::load_history`. The `[harvest.sqlite]` section is refused in a
build without the feature. No `WorkflowEvent` variant, no migration.

Tests: `autumn-harvest-plugin/tests/sqlite_plugin.rs` boots real apps on a
temp directory: an end-to-end run over HTTP, the `501` fallback, a refused
start option, the 401 gate outside `dev`, the app handle, a build refusal,
`sqlite_path` on an in-memory app database, and a restart that resumes a
parked run. `autumn-harvest-sqlite`'s
`shared_state` suite covers the state map in every context. CI runs the
plugin suite in the new `sqlite-plugin` job. See `docs/sqlite-plugin.md`.
