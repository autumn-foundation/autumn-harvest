# HarvestPlugin on SQLite

`HarvestPlugin` runs on two backends. Postgres is the default. The `sqlite`
feature runs the plugin on [`autumn-harvest-sqlite`](sqlite-backend.md)
instead, for an Autumn app built on `autumn-web/sqlite`. This includes a
SQLite fleet app.

Epic [#2466](https://github.com/autumn-foundation/autumn-harvest/issues/2466)
tracks the work. This page describes slice 1,
[#2467](https://github.com/autumn-foundation/autumn-harvest/issues/2467).

## Turn it on

```toml
[dependencies]
autumn-web = { version = "0.9", features = ["sqlite"] }
autumn-harvest = { version = "0.8", default-features = false }
autumn-harvest-plugin = { version = "0.8", features = ["sqlite"] }
```

The feature is a backend flip, like `autumn-web/sqlite`, which it turns on.
Only an end application may enable it. Cargo unifies features across the
build graph. A library that enables it flips `autumn-web` to SQLite for every
crate in the build.

The app code is the same as on Postgres:

```rust,ignore
autumn_web::app()
    .plugin(
        HarvestPlugin::new()
            .workflows(workflows![offboard_hive])
            .activities(activities![snapshot_hive, purge_hive])
            .state(ArchiveDir("archive".into()))
            .api("/harvest"),
    )
    .run()
    .await;
```

## Where the data lives

Harvest has its own SQLite file. It sits next to the app database:

| `database.url` | Harvest file |
| --- | --- |
| `sqlite://apiary-control.db` | `apiary-control.harvest.db` |
| `sqlite:///srv/app.db` | `/srv/app.harvest.db` |
| `file:/srv/app.db?mode=rwc` | `/srv/app.harvest.db` |

In a fleet app, `database.url` is the control database, so Harvest sits beside
it. No fleet file gets Harvest tables.

Set the file explicitly with `harvest.sqlite.path` or
`AUTUMN_HARVEST_SQLITE__PATH`, or in code with
`HarvestPlugin::sqlite_path(path)`. The operator setting wins over the code
value. An in-memory app database has no file to sit next to, so it needs one
of them. Each app in one process, as in a test binary, needs its own file.

```toml
[harvest.sqlite]
path = "/var/lib/app/harvest.db"
# How often the driver fires due timers, retries and schedule slots.
tick_ms = 250
```

Why a separate file: `SqliteRuntime` holds an exclusive writer lock on its
file (issue #1834). The lock would block the app's own pool on a shared file.
A separate file also lets a backup of the app database leave the workflow
history out, and the reverse.

The plugin registers no migration set on a SQLite app. The Postgres sets would
fail on SQLite. `autumn-harvest-sqlite` applies its own schema when it opens
its file.

## How it runs

```mermaid
flowchart LR
    H[HTTP handlers] -->|commands| C[(mpsc)]
    A[App code: HarvestSqlite] -->|commands| C
    C --> D[driver task<br/>owns SqliteRuntime]
    D -->|poll_once on each tick and after each command| F[(app.harvest.db)]
    H -->|list, describe, history| R[SqliteReader<br/>read-only]
    A -->|list, describe, history| R
    R --> F
```

- **One writer.** One task on the app's Tokio runtime owns the
  `SqliteRuntime`. Writes reach it over a channel and run one at a time. An
  activity runs on the app's runtime, so a task that it spawns, such as a
  fleet open, lives there too. A long activity delays the next write.
- **A tick instead of `LISTEN`/`NOTIFY`.** The driver drives every run after
  each batch of at most 32 commands, then waits for the next command or the
  tick. The batch bound keeps a steady stream of requests from starving the
  runs. The tick bounds
  how late a due timer, a retry backoff or a schedule slot fires.
- **Reads do not wait.** List, describe and history use a read-only
  `SqliteReader`, so a long activity never blocks them.
- **A query replays off the driver.** The driver reads the rows. The replay
  runs on the caller's task.
- **Shutdown.** The plugin's shutdown hook stops the driver and releases the
  writer lock. `HarvestSqlite::shutdown()` does the same from app code, for
  example in a test, where `TestApp` runs no shutdown hook.
- **Restart resumes.** A task that a crash left `RUNNING` is reclaimed when the
  file opens, so its activity runs again. Activities are at least once, as on
  Postgres.

## App state in activities

`ctx.state::<T>()` reads the same map in a workflow, an activity, a local
activity and a query replay:

- every value given to `HarvestPlugin::state(..)`;
- `AppState`, which the plugin adds at startup, as the Postgres worker does.

`AppState` carries what exists only after boot, such as the app pool and the
fleet:

```rust,ignore
#[activity]
async fn purge_hive(ctx: &ActivityContext, hive: String) -> Result<(), String> {
    let state = ctx.state::<AppState>().ok_or("no app state")?;
    let fleet = state.shards().and_then(|s| s.fleet()).ok_or("no fleet")?;
    // ...
}
```

## From app code

The plugin installs a `HarvestSqlite` handle at startup. Take it as an
extractor, or read it from the app state:

```rust,ignore
use autumn_harvest_plugin::sqlite::{HarvestSqlite, StartOptions};

#[post("/hives/{name}/offboard")]
async fn offboard(harvest: HarvestSqlite, Path(name): Path<String>) -> AutumnResult<String> {
    let started = harvest
        .start("offboard_hive", json!({ "hive": name }), StartOptions::new().workflow_id(&name))
        .await
        .map_err(|e| AutumnError::service_unavailable_msg(e.to_string()))?;
    Ok(started.exec_id.to_string())
}
```

The handle has `start`, `signal_with_start`, `signal`,
`signal_by_workflow_id`, `cancel`, `terminate`, `query`, `update`, `describe`,
`list`, `history` and `shutdown`. The schema checks follow Postgres:

- `start` checks the workflow's `input_schema` first (issue #373).
- `signal_with_start` checks it only when it creates a run. An attach
  discards the input, so it needs none.
- A signal checks the `arg_schema` of a declarative `#[signal]` handler
  (issue #610), after the committed key probe. A retry of a delivered keyed
  signal stays a duplicate, even when the schema changed since.

A violation is `HarvestSqliteError::InvalidInput` or `InvalidSignal`. Over
HTTP it is the Postgres `400` with `violations`.

## The management API

`.api(path)` mounts these routes. Each one keeps its Postgres path and field
names. A body carries the fields this backend stores, so it can be a subset
of the Postgres body.

| Route | Notes |
| --- | --- |
| `GET /health`, `/health/live`, `/health/ready` | `backend: "sqlite"` |
| `GET /workflows` | `workflow_name`, `workflow_id`, `workflow_id_prefix`, `state`, `limit`, `page_size`, `cursor` |
| `GET /workflows/registered` | |
| `GET /workflows/{id}` | the run and its whole history |
| `GET /workflows/{id}/history` | |
| `POST /workflows/{workflow_name}/start` | `workflow_id`, `input`, `memo`, `search_attrs`, `reuse_policy` |
| `POST /workflows/{workflow_name}/signal-with-start` | |
| `POST /workflows/{id}/signal/{signal_name}` | `Idempotency-Key` header or `idempotency_key` query |
| `POST /workflows/{id}/cancel`, `/terminate` | optional `reason` |
| `GET`/`POST /workflows/{id}/query/{query_name}` | |
| `POST /workflows/{id}/update/{update_name}` | runs in the request and answers `200` with the output |
| `GET /workflows/by-id/{workflow_name}/{workflow_id}` | |
| `POST /workflows/by-id/{workflow_name}/{workflow_id}/signal/{signal_name}` | |

Every other management route answers `501` and links the epic. That
includes `GET /workflows/{id}/result`: its Postgres polling contract (`204`
with `Retry-After` while running, the compact state, the successor chain) is
not ported yet ([#2489](https://github.com/autumn-foundation/autumn-harvest/issues/2489)).
Poll `GET /workflows/{id}` and read `execution.state` and `execution.output`.

A route that reads no query parameter refuses one with a `400`. For example,
`GET /workflows/{id}/history` does not page yet, so `?after=`, `?event_type=`
and `?limit=` are refused. They are not ignored, so the full history never
passes for a filtered page (#2489). A start
option this backend cannot honour, such as `queue`, `priority`, `start_at` or
the `Idempotency-Key` header, is a `400` that names it. Neither is ever an empty `200`.

Auth works as on Postgres. `api_with_auth(path, mw)` wraps the router. The
issue #1802 mutation gate refuses an anonymous mutating call outside `dev`.
`allow_unauthenticated_mutations()` opens it. `reuse_policy =
"terminate_if_running"` needs harvest admin access.

## What slice 1 refuses

Each of these fails the build or the boot and names its issue. A silent
no-op would let an app believe a protection or a feature is on.

| Option or mode | Issue |
| --- | --- |
| `enable_api_tokens`, `api_with_role_auth`, `with_roles`, `with_authorizer`, `with_api_rate_limit`, `api_with_oidc` | [#2469](https://github.com/autumn-foundation/autumn-harvest/issues/2469) |
| Vantage, `decode_payloads_on_read`, the other read routes, declarative `queries![..]` (a query that the workflow registers with `ctx.register_query` works) | [#2468](https://github.com/autumn-foundation/autumn-harvest/issues/2468) |
| The transactional outbox | [#2470](https://github.com/autumn-foundation/autumn-harvest/issues/2470) |
| Workflow schedules, `retention`, `mcp_tools`, `mcp_tasks`, `webhooks`, `connector`, `synthetic_canary` | [#2471](https://github.com/autumn-foundation/autumn-harvest/issues/2471) |
| `harvest.mode = "split"` / `"external"`, per-tenant Harvest in a fleet | [#2472](https://github.com/autumn-foundation/autumn-harvest/issues/2472) |
| `harvest.worker_enabled = false` | not planned: the app process is the only writer |
| DAGs | an engine non-goal of the SQLite backend (#2019) |
| `with_metrics_scrape` | the epic |

These options do nothing on SQLite, because they tune Postgres machinery that
this backend does not have: `worker(..)`, `load_shed`, `ramp_guard`,
`start_idempotency_window`, `workflow_log_persistence`, the durable-route
limits and the status thresholds.

## Tests

`autumn-harvest-plugin/tests/sqlite_plugin.rs` boots a real app on a temp
directory, with no Postgres and no Docker:

```bash
cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin
```

CI runs it in the `sqlite-plugin` job.
