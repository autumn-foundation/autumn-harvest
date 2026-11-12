# HarvestPlugin on SQLite

`HarvestPlugin` runs on two backends. Postgres is the default. The `sqlite`
feature runs the plugin on [`autumn-harvest-sqlite`](sqlite-backend.md)
instead, for an Autumn app built on `autumn-web/sqlite`. This includes a
SQLite fleet app.

Epic [#2466](https://github.com/autumn-foundation/autumn-harvest/issues/2466)
tracks the work. This page describes slice 1,
[#2467](https://github.com/autumn-foundation/autumn-harvest/issues/2467),
[#2472](https://github.com/autumn-foundation/autumn-harvest/issues/2472):
split mode and per-tenant Harvest, and
[#2469](https://github.com/autumn-foundation/autumn-harvest/issues/2469):
API tokens, roles, OIDC and the audit log. The design records are
[`DESIGN-2472.md`](design/DESIGN-2472.md) and
[`DESIGN-2469.md`](design/DESIGN-2469.md).
[#2468](https://github.com/autumn-foundation/autumn-harvest/issues/2468)
ports the read routes, Vantage and declarative queries. Its design record
is [`DESIGN-2468.md`](design/DESIGN-2468.md).
[#2470](https://github.com/autumn-foundation/autumn-harvest/issues/2470)
adds the [transactional outbox](#the-transactional-outbox). Its design
record is [`DESIGN-2470.md`](design/DESIGN-2470.md).
[#2471](https://github.com/autumn-foundation/autumn-harvest/issues/2471)
ports schedules, retention, MCP, webhooks and connectors. Its design record
is [`DESIGN-2471.md`](design/DESIGN-2471.md).

## Turn it on

```toml
[dependencies]
autumn-web = { version = "0.10", features = ["sqlite"] }
autumn-harvest = { version = "0.10", default-features = false }
autumn-harvest-plugin = { version = "0.10", features = ["sqlite"] }
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
it. No fleet file gets Harvest tables. With
[`sqlite_per_tenant()`](#per-tenant-harvest-in-a-fleet), each tenant also gets
its own Harvest file.

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
its file. The one Harvest table in the app database is the
[outbox](#the-transactional-outbox). The startup hook creates it.

## Split and external mode

A SQLite app can keep Harvest in a Postgres database. Set the mode and the
Harvest database URL, as on Postgres:

```toml
[harvest]
mode = "split"            # or "external"

[harvest.database]
url = "postgres://harvest:secret@db.internal:5432/harvest"
```

The plugin then takes its Postgres path. That path serves the whole
Postgres management API, Vantage and every builder option. The SQLite
slice limits on this page do not apply. App code uses the Postgres client,
not `HarvestSqlite`.

- **Its own pool.** The plugin builds a Postgres pool from
  `harvest.database.url`. Each connection follows the DSN's `sslmode`, as
  in [`pg_tls`](../autumn-harvest/src/pg_tls.rs). Those rules are stricter
  than in `autumn-web` and libpq: `sslmode=require` checks the certificate
  chain and the host name, and `sslrootcert` is not read. Point
  `SSL_CERT_FILE` at a private CA. The boot opens one connection and logs
  an error that names this remedy when it fails.
- **Migrations.** The `dev` profile applies the Harvest migrations to that
  database. Elsewhere, run `harvest migrate run` against it.
- **The app pool stays SQLite.** No Postgres migration set goes to the app
  database. The transactional outbox relay does not run, and activities get
  no `AppDbPool`. An activity reads the SQLite app pool through
  `ctx.state::<AppState>()`. The SQLite outbox runs in embedded mode only
  ([#2470](https://github.com/autumn-foundation/autumn-harvest/issues/2470)).
- **Loud conflicts.** `harvest.sqlite.path` and `sqlite_per_tenant()` have
  no effect in this mode, so the build refuses them.
  `HarvestPlugin::sqlite_path` is a default for embedded mode. It only logs
  a warning.

## Per-tenant Harvest in a fleet

`sqlite_per_tenant()` runs one Harvest for each open database of the app's
SQLite fleet (autumn ADR 0019). A fleet in `slot` mode works the same way,
with one Harvest per slot database. The fleet database is the isolation
unit: in `slot` mode, every tenant that routes to one slot shares its
Harvest, as it shares the slot's app data.

```rust,ignore
HarvestPlugin::new()
    .workflows(workflows![process_order])
    .activities(activities![charge_card])
    .sqlite_per_tenant()
    .api("/harvest")
```

| Tenant database | Its Harvest file |
| --- | --- |
| `{root}/{bucket}/acme.db` | `{root}/{bucket}/acme.harvest.db` |
| `{root}/{bucket}/slot-00042.db` | `{root}/{bucket}/slot-00042.harvest.db` |

A tenant id and a slot number hold no `.`, so the fleet never lists a
Harvest file as a database.

```mermaid
sequenceDiagram
    participant F as DatabaseFleet
    participant L as Harvest lifecycle
    participant D as tenant driver
    F->>L: on_open(acme)
    L->>D: open acme.harvest.db, start, register as a worker
    F->>D: close: stop the worker, wait for it
    F->>L: on_close(acme): drop the handle
    F->>L: on_delete(acme): remove acme.harvest.db
```

- **The fleet decides.** The plugin installs a `FleetLifecycle`. An open
  starts the tenant's driver. A close stops it, after the command or pass in
  flight. The plugin never keeps a tenant open by itself.
- **The control Harvest stays.** It runs beside the tenant Harvests, for
  platform workflows such as a tenant purge. `HarvestSqlite` is the control
  Harvest.
- **The same registry.** Every tenant runs the workflows and activities that
  the plugin registers.
- **The tenant key.** `HarvestTenant::current()` gives the fleet database
  key (a tenant or a slot) inside an activity that a tenant driver runs. It is `None` on the control
  driver, and on a task that an activity spawns.
- **A delete erases.** A fleet delete removes the tenant's Harvest file and
  its `-wal`, `-shm` and `-journal` sidecars. The `.lock` file stays.
- **No fleet, no boot.** The boot fails when the app has no
  `[database.fleet]`.
- **Shutdown.** The plugin's shutdown hook releases the shared registry, so
  a stopped app frees its state. Tenants that open after it get no driver.

App code reaches a tenant through `HarvestFleet`, an `AppState` extension
and an axum extractor:

```rust,ignore
use autumn_harvest_plugin::sqlite::{HarvestFleet, StartOptions};

#[post("/{tenant}/orders")]
async fn create(fleet: HarvestFleet, Path(tenant): Path<String>) -> AutumnResult<String> {
    let harvest = fleet.open_for(&tenant).await.map_err(to_http)?;
    let run = harvest
        .start("process_order", json!({}), StartOptions::new())
        .await
        .map_err(to_http)?;
    Ok(run.exec_id.to_string())
}
```

`open_for` takes the app's routing key. `open` takes a `FleetDbKey`. Both
open a closed tenant. Both follow the fleet's `create_on_demand`, so they
create a tenant only when the fleet does. The handle holds a lease, so the tenant stays open
while any clone of it lives. The fleet owns the tenant driver:
`shutdown()` on a tenant handle is an error.

`.api(path)` also serves every core route on one tenant, under
`{path}/tenants/{key}/..`. `{key}` is the routing key. For example,
`POST /harvest/tenants/acme/workflows/process_order/start`. These routes
never create a tenant, whatever `create_on_demand` says. A tenant that does
not exist is `404`. A route opens its tenant as background work: it never
goes past `max_open` and never evicts a tenant that the app uses. A full
fleet is `503`. The fleet rules the rest: a bad key is `400` and a tenant
in a delete is `410`. Without `sqlite_per_tenant()` they answer `404`.

The auth layer and the #1802 mutation gate cover these routes too. The gate
passes a read, and a read opens its tenant, which starts its driver. Put the
API behind `api_with_auth` outside `dev`.

### Limits

- A closed tenant runs no timer, retry or activity. The next open resumes
  its runs. The fleet sweeper wakes a tenant for outbox, hook and backfill
  work only.
- A takeover loses the runs that the old owner did not ship: at most one
  sync interval of work.
- A direct `DatabaseFleet::restore` to a point in time restores the
  Harvest file at its latest state. Use `HarvestFleet::restore` for one
  target on both files.
- On Unix the database identity is the device and inode. Elsewhere it is
  the creation time, which Windows can keep across a rename. A direct
  `DatabaseFleet::restore` on Windows can then keep the old Harvest file.
  Use `HarvestFleet::restore` there: a finished restore commits its target,
  and the next open applies it whatever the identity says.
- An import or a re-create gives the tenant a new database file. A Harvest
  file with no replica then goes aside, as `<file>.aside-<n>`.
- `DatabaseFleet::release_ownership` does not wait for a Harvest backlog.
  The shutdown hook ships it within the fleet's final flush, and logs each
  tenant that stays behind.

### Replication

When the fleet replicates (autumn ADR 0019 §5), each tenant Harvest file
ships with its tenant database. It uses the same engine, destination and
ownership epoch, under the tenant prefix:

| Object | Prefix |
| --- | --- |
| The tenant database | `{root}/fleet/tenant/{id}/` |
| Its Harvest file | `{root}/fleet/tenant/{id}/harvest/` |

The plugin builds the destination from `[replication]`. App code that
builds the fleet replication gives the same destination and settings to
`HarvestPlugin::sqlite_tenant_replica(TenantReplica::new(destination,
settings))`.

```mermaid
sequenceDiagram
    participant F as DatabaseFleet
    participant L as Harvest lifecycle
    participant R as replica
    F->>L: on_open(acme, epoch)
    L->>R: restore a missing or stale acme.harvest.db
    L->>L: open the file, record the epoch, start the driver
    loop each sync interval
        L->>R: ship, if this host holds the epoch
    end
    F->>L: on_close(acme): ship until caught up
```

- **A takeover keeps the runs up to the last ship.** A host that opens a
  tenant restores a missing Harvest file before the driver starts, under
  the fleet's `restore_missing`.
- **The Harvest file follows its database.** The file records the identity
  of its tenant database file. A restore gives a new database file. The
  next open then restores the Harvest file from the replica.
- **A stale file never opens.** The file records the newest epoch that it
  opened under. When the replica holds a newer epoch of another owner, the
  open replaces the local file first.
- **A host that lost the range ships nothing.** Each tick checks the
  ownership epoch. A close also reads the epochs at the destination. A
  restore takes the newest epoch first, so a later file of an old owner
  never wins.
- **A delete covers both files.** The tombstone marker sits under the
  tenant prefix, and the purge removes both replicas. A re-created tenant
  gets no deleted run back.
- **A restore covers both files.** `HarvestFleet::restore(key, target)`
  restores the database. It keeps the target on disk, and the next open
  restores the Harvest file to the same point. When the Harvest replica no
  longer reaches the target, each open refuses until a new restore.
- **One checkpointer.** A shipped file opens with
  `wal_autocheckpoint = 0`. The replicator checkpoints only what it has
  shipped.

`HarvestFleet::tick_replicas()` ships once, for tests. It blocks, so call
it on a blocking thread. `HarvestPlugin::sqlite_tenant_replica` needs
`sqlite_per_tenant()`.

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

## The durable agent

`HarvestPlugin::agent(harness)` hosts the durable agent of
`autumn-harvest-agent`. It calls `autumn_harvest_agent::register`, so the
app needs no `SqliteRuntime` of its own. The driver gives the harness to
each agent activity as state:

```rust,ignore
use autumn_harvest_agent::{AgentHarness, AgentId, AgentOp, Approval};
use autumn_harvest_plugin::sqlite::{HarvestSqlite, agent};

let plugin = HarvestPlugin::new()
    .agent(AgentHarness::new(model).agent(profile))
    .api("/harvest");

// In a handler that takes `harvest: HarvestSqlite`:
let id = AgentId::new("support", "acct-42")?;
agent::send(&harvest, &id, &AgentOp::message("hello"), Some("m-1")).await?;
let state = agent::state(&harvest, &id).await?;
agent::decide(&harvest, &id, &signal, &Approval::Approve, Some("d-1")).await?;
```

The module `sqlite::agent` mirrors the durable agent calls of
`autumn_harvest_agent::sqlite`:

- `send` starts the agent when it has no run. A steer or an interrupt goes
  first, to the running run.
- `delete` ends the agent once its inbox is empty.
- `current_run` and `state` read the running run. With no run they return
  `None`.
- `decide` sends a decision to the approval wait that its signal names. The
  approval notice of the harness names the signal. For a one-shot
  `agent_loop` run, call `HarvestSqlite::signal` with its execution id.
- `reload_profiles` sends `Control::ReloadProfile` to each running agent
  of a kind. Each agent then reads the profile of the harness again. Call
  it after a restart with a new harness. An ended agent stays ended.

No call drives the run. The driver runs each op, so a long model turn
delays the next write. An activity runs on the driver task, so make these
calls from app code. A call from an activity body can wait for its own
driver and hang.

SQLite keeps no run tenant, so an answer check admits the tenant that the
task declares. The agent keys its memory and its workspace by agent id,
with no tenant. So the build refuses `agent` with `sqlite_per_tenant()`.

The agent reads its profile overlays through the store of the harness,
such as `SqliteOverlayStore` in a file of its own. The
`/agents/{kind}/profile` routes still need Postgres, so the build refuses
`agent_overlay_store`.

## The transactional outbox

A start from the handle commits in the Harvest file, not in your business
transaction. To start a run only when your rows commit, write the start to
the outbox on the connection of that transaction (issue #2470):

```rust,ignore
use autumn_harvest_plugin::WorkflowStartRequest;
use autumn_harvest_plugin::sqlite::enqueue_workflow_start_outbox;
use diesel_async::AsyncConnection as _;

conn.transaction(async |conn| {
    insert_order(conn, &order).await?;
    enqueue_workflow_start_outbox(conn, &WorkflowStartRequest {
        workflow_name: "process_order".to_owned(),
        workflow_id: order.id.clone(),
        queue_name: "default".to_owned(),
        input: json!(order),
        memo: None,
        search_attrs: None,
    })
    .await
})
.await?;
```

- **The table.** The startup hook creates `harvest_workflow_outbox` in the
  app's primary database. In a fleet app, that is the control database.
  A fleet tenant database has no outbox table, so a write there fails.
  `OUTBOX_SCHEMA_SQL` holds the DDL, for an app that applies it before the
  deploy. A table of another shape fails the boot.
- **The relay.** It drains the due rows every
  `harvest.outbox.poll_interval_ms`, in batches of `batch_size`. The control
  Harvest runs each start. A failed start keeps `last_error`, and backs off
  with `base_retry_delay_ms` and `max_retry_delay_ms`. `claim_ttl_ms` has no
  effect: one process holds the Harvest file, so one relay runs.
  `harvest.outbox.enabled = false` stops the relay, and the rows wait. Two
  apps with two Harvest files must not share one app database.
- **Exactly once.** A rolled-back write leaves no row. A committed row runs
  once, across a crash before the relay and a crash between the start and
  the mark. The relay starts a row only when its
  `(workflow_name, workflow_id)` has no run, in any state. This is stricter
  than Postgres: a key that an API start already used, or that an operator
  terminated, is marked with that run and does not run again.
- **Flush.** `flush_workflow_start_outbox(&state)` and
  `drain_workflow_start_outbox_once` drain the SQLite outbox, as on
  Postgres. An activity or a workflow cannot call them: they run on the
  driver task, and the start would wait for that task.
- **Refusals.** The writer refuses an empty or blank `workflow_id`, a
  `queue_name` other than `"default"`, and `search_attrs` that is not an
  object of JSON primitives. A duplicate key is a no-op. An unknown workflow
  or an input over the cap fails at start, and the relay retries it, as on
  Postgres.
- **Split and external mode.** No SQLite relay runs. The boot warns when
  the app database holds undelivered starts from embedded mode.
- **In-memory app database.** Its pool has one connection. A flush then
  waits while the caller holds that connection. Use a file.

## The management API

`.api(path)` mounts these routes. Each one keeps its Postgres path, status
codes and field names (#2489). A body carries the fields this backend
stores, so it can be a subset of the Postgres body.

| Route | Notes |
| --- | --- |
| `GET /health`, `/health/live`, `/health/ready` | `/health` adds `backend: "sqlite"` and `path`. `/health/ready` reads the file |
| `GET /workflows` | `workflow_name`, `workflow_id`, `workflow_id_prefix`, `state`, `limit`, `page_size`, `cursor` |
| `GET /workflows/registered` | |
| `GET /workflows/{id}` | the run and the first 100 events |
| `GET /workflows/{id}/history` | `after`, `limit`, `event_type` |
| `GET /workflows/{id}/result` | `wait` |
| `POST /workflows/{workflow_name}/start` | `workflow_id`, `input`, `memo`, `search_attrs`, `reuse_policy` |
| `POST /workflows/{workflow_name}/signal-with-start` | |
| `POST /workflows/{id}/signal/{signal_name}` | `Idempotency-Key` header or `idempotency_key` query |
| `POST /workflows/{id}/cancel`, `/terminate` | optional `reason` |
| `GET`/`POST /workflows/{id}/query/{query_name}` | |
| `POST /workflows/{id}/update/{update_name}` | runs in the request and answers `200` with `update_id` and `output` |
| `GET /workflows/by-id/{workflow_name}/{workflow_id}` | also `/result` |
| `POST /workflows/by-id/{workflow_name}/{workflow_id}/signal/{signal_name}` | |
| `GET /workflows/count` | `group_by`, `workflow_name`, `state`, `started_after`, `started_before`, `limit_groups` (#2468) |
| `GET /workflows/summaries` | admin only. Always empty: this backend never demotes a run |
| `GET /workflows/registered/{name}/schema`, `/interface` | |
| `GET /workflows/{id}/children` | `status`, `workflow_name`, `limit`, `cursor`, `depth`. Also by-id |
| `GET /workflows/{id}/tree` | `max_depth`, `max_nodes`, `summary` |
| `GET /workflows/{id}/run-chain` | |
| `GET /workflows/{id}/timeline` | |
| `GET /workflows/{id}/logs` | admin only. `limit`, `cursor`, `level`, `since` |
| `GET /workflows/{id}/awaitables` | admin only |
| `GET /workflows/{id}/stack` | also by-id |
| `GET /workflows/{id}/history/export` | `payload_policy`, `max_bytes` |
| `GET /ui/workflows`, `/ui/workflows/{id}` | [Vantage](#vantage) |
| `GET`/`POST /admin/tokens`, `DELETE /admin/tokens/{id}` | the Postgres handlers. See [Auth](#auth-tokens-roles-and-audit) |
| `GET /admin/audit` | the Postgres handler and filters |
| `GET /admin/schedules`, `GET /admin/schedules/{id}` | [Schedules](#schedules-and-retention) (#2471) |
| `POST /admin/schedules/workflow` | upserts, and answers `201` |
| `PATCH`, `DELETE /admin/schedules/{id}` | a `PATCH` of a code schedule sets an override |
| `POST /admin/schedules/{id}/pause`, `/resume` | |
| `POST /admin/schedules/{id}/trigger` | admin only. `?force=true` triggers a paused schedule |
| `GET /admin/retention`, `POST /admin/retention/run-now` | admin only. [Retention](#schedules-and-retention) |

`GET /workflows/{id}/result` follows the Postgres polling contract. A run
that is not terminal answers `204` with `Retry-After: 1`. A terminal run
answers the compact form, with a lower-case `state`. The read follows the
`continue_as_new` chain and the workflow retry chain to the last run. `wait`
polls the file until the run ends or the wait elapses.

`GET /workflows/{id}/history` pages with the Postgres cursor. An event `id`
is its 0-based `event_id` plus one, so `after=0` reads from the first event.
`timestamp` is the append time. An event written before #2489 has a `null`
`timestamp`.

A signal, cancel, terminate, query or update to a `FAILED` run with a live
retry acts on the retry, as on Postgres (#843). A signal names the retry in
`routed_execution_id`.

`tests/support/wire_contract.rs` holds the checks of this contract. The
SQLite suite and the Postgres integration tests run them both.

Each read route of #2468 reads the file through `SqliteReader`, never the
driver. It then calls the same pure step as its Postgres route, so one
function gives the shape on both backends. One file is one shard: a
fan-out body reads `status: "complete"`, and a row carries `shard_id: 0`.

A declarative `queries![..]` handler answers `GET` and `POST
/workflows/{id}/query/{name}`, as on Postgres. The drive, the query replay
and the awaitables replay all register it.

`HarvestPlugin::workflow_log_persistence` stores the `ctx.log_*` lines in
the Harvest file. `GET /workflows/{id}/logs` reads them. The cap and the
marker follow Postgres. The write is best effort, as on Postgres: a failed
write drops the lines of that cycle and never fails the cycle.

Every other management route answers `501` and links the epic.

## Schedules and retention

Issue #2471 ports both. The driver runs them on the Harvest file, so they
add no second writer.

### Schedules

Declare a schedule in code with `HarvestPlugin::workflow_schedule`. The same
call works on Postgres:

```rust,ignore
HarvestPlugin::new()
    .workflows(workflows![nightly_report])
    .workflow_schedule(WorkflowSchedule::new(
        "nightly_report",
        Schedule::Cron("0 3 * * *".into()),
    ))
    .api("/harvest")
```

- **Code schedules.** The control Harvest upserts each one at boot. A
  boot keeps a stored slot and a stored pause. An unknown workflow or a
  field that SQLite cannot honour fails the boot and names it.
- **`harvest.scheduler_enabled = false`.** No slot fires and no code
  schedule is upserted, as on Postgres, where the scheduler does both. The
  admin routes still change the rows, and a trigger still starts a run.
  An unset switch is on for every process role, as the driver is (#2477).
  The app process is the only writer, so no other process fires its slots.
- **Admin routes.** `{id}` is a UUID v5 of the workflow name, so it is
  stable for that name. `POST /admin/schedules/workflow` creates or replaces
  an API schedule. A `PATCH` of an API schedule edits it in place. A `PATCH`
  of a code schedule sets an operator override (#2390), and
  `clear_override` returns it to code. A clear stands alone, as on Postgres.
  A `DELETE` of a code schedule is a `400`, because the next boot upserts it
  again. Each write adds one audit row. A tenant row names its tenant in
  `route_or_command`.
- **Pause.** `POST /admin/schedules/{id}/pause` stores its `reason` as
  `pause_reason`. A resume clears it.
- **Trigger.** `POST /admin/schedules/{id}/trigger` starts one run now with
  the stored input. Under `skip`, a schedule at its run limit answers
  `outcome: "skipped_overlap"`. The run counts against the next slot. Its
  `workflow_id` is `manual-<workflow name>-<millis>-<nonce>`. Postgres puts
  the schedule id where this backend puts the workflow name.
- **Supported fields.** A cron or interval expression with its time zone,
  `input`, `paused`, `max_active_runs`, and the `skip` and `allow_all`
  overlap policies. Each other field is a `400` that names it, unless it has
  the value that `GET` shows, such as `catchup: false` or `jitter_secs: 0`.
  Catch-up is always off. The engine ignores the jitter of a code schedule,
  because one writer has no fleet to spread. The admin routes refuse a
  `jitter_secs` above zero.
- **Not ported.** Backfill, `/runs`, `/decisions` and `/preview` answer
  `501`.

A tenant driver fires the schedules that its own file stores. Only the
control Harvest gets the code schedules.

### Retention

`HarvestPlugin::retention(..)` purges old terminal runs on SQLite:

| `RetentionConfig` field | On SQLite |
| --- | --- |
| `max_age_secs`, `overrides` | honoured: a type override wins, and a type with neither stays |
| `dry_run` | honoured: a pass counts and removes nothing. It counts at most 2,048 runs |
| `tick_interval_secs` | the interval of the pass. The first pass runs at boot |
| `audit_retention_days` | honoured by the token and audit store |
| `tenant_overrides`, `summary`, `payload_orphan_grace_secs` | refused at build: SQLite has no run tenant, no summary table and no payload store |

The other fields tune Postgres machinery, such as partitions and the
task-queue janitor. They change nothing here.

- **One writer.** The pass runs on the driver in steps of eight batches of
  256 runs. The driver yields between steps, so a large backlog never holds
  it for long. A pass removes the events, tasks, timers, signals, attempts,
  ledger rows, logs and row of each run.
- **`GET /admin/retention`** answers the Postgres `RetentionStatus`, with
  one shard: `deleted_count`, `candidate_count`, `deleted_by_workflow` and
  `last_error` of the last finished pass. A dry run counts its would-be
  deletes in `deleted_count`, as on Postgres. `oldest_age_secs_skipped` is
  always `null`.
- **`POST /admin/retention/run-now`** queues a pass, as on Postgres. The
  driver runs it on its next turn.
- **Every role.** The driver runs the pass on every process role. It
  ignores `[harvest.retention] run_on`, which places the Postgres janitor
  (#2478). The app process is the only writer, so no other process runs
  the pass.
- Each tenant file runs the same policy, with its own status.

## MCP, webhooks and connectors

Issue #2471 ports them. Each one starts its runs through the
`HarvestSqlite` handle of the control Harvest, not the Postgres start path.
The descriptors, the auth layers and the response shapes are the Postgres
ones.

- **MCP tools.** `start_`, `{wf}_status`, `signal_`, `{wf}_update_` and
  `{wf}_watch` call the SQLite management handlers. `status` and `watch`
  follow the retry and continue-as-new chain. `watch` polls the file every
  100 ms, because SQLite has no `LISTEN`. It sends a progress frame on each
  change of state. This backend stores no `current_details`, so a frame
  names the state, and `status` reads `current_details: null`.
  The plugin needs its `mcp` feature and `.mcp_tools()`. Without them, the
  plugin serves no tools and logs one `WARN` at build (issue #2609).
- **MCP tasks.** A task reads its live run and its open signal waits from
  the file. An answer is delivered with its input key as the signal key, so
  a retried answer is a no-op. A start key in `Idempotency-Key` or the task
  `_meta` is refused, as on the start route. SQLite has no start-idempotency
  table, so a retried start would run twice.
- **Webhooks.** A delivery starts or signals through the handle, and
  answers the Postgres envelope. A `SignalsWithStart` target dedupes on the
  binding and the delivery id. Each dispatch writes one audit row to the
  Harvest file.
- **Connectors.** The consumer loops start after the driver, and stop
  before it. A poison message lands in `harvest_connector_dead_letters` in
  the Harvest file. A redelivery adds no second record. The loops record no
  metric, because SQLite has no telemetry registry.

| Refused at build | Why |
| --- | --- |
| `#[webhook(queue = ..)]`, `SourceBinding::queue` | one writer drains every queue |
| a `Starts` connector binding in `BrokerCoordinates` mode, the default | SQLite has no start-idempotency table. Set `IdempotencyMode::WorkflowId` |

Before startup each surface answers `503`, never the Postgres path. A
webhook `503` releases its replay key, so the provider retries.

## Vantage

`.api(path)` also serves Vantage under `{path}/ui`, read-only. A tenant
route serves it under `{path}/tenants/{key}/ui`:

- `ui/workflows`: the runs, newest first, with the state, workflow and
  workflow id prefix filters, and a next-page link. A bad filter shows
  inline and is not applied.
- `ui/workflows/{id}`: the run, its input, output and error, its first 50
  children, and its history in pages of 100 events. It links to the JSON
  routes.

The pages link only to SQLite pages. The Postgres pages for DAGs, workers,
schedules and dead letters read the Postgres shape of those tables, so they
stay off. The pages hold no form that writes.

### Where the contract differs

- A cancel is cooperative. It answers `202` with `state: "RUNNING"` and
  `deferred: true`. The next drive ends the run. A repeat cancel answers
  `newly_cancelled: false` and the first reason.
- A route refuses a query parameter that it does not read with a `400`.
  Postgres ignores it. `/history` ignores it, as on Postgres. `/result`
  refuses it on both backends. A start option this backend cannot honour, such as
  `queue`, `priority`, `start_at` or the `Idempotency-Key` header, is a `400`
  that names it. Neither is ever an empty `200`.
- An empty request body is the default body. Postgres needs a JSON body.
- A signal idempotency key dedupes across the whole run chain. Postgres
  dedupes it per run.
- `workflow_id` and `workflow_id_prefix` filter a listing on this backend
  only.
- An update completes in the request. `?wait=admitted` and `timeout_secs`
  are refused.
- An event written before #2489 has `timestamp: null`. A Postgres event
  always has a time.
- A start or a signal-with-start checks its body before the workflow name.
  So a bad body is a `400` where Postgres answers `404` for an unknown name.
- A cancel or a terminate cuts its trimmed reason to 500 characters. The run
  keeps the reason in its cancel state and in history, so the cap bounds that
  storage. Postgres keeps the whole reason.
- A cancel or a terminate body has no `failed_task_count`. This backend
  deletes the pending tasks of the run. It does not mark them failed.
- `GET /workflows/summaries` is always empty. A purge removes a whole run,
  so no run is ever a summary.
- A run chain gives each run its execution id as `run_id`. This backend
  has no separate run id. A key with more than 256 runs answers the queried
  run alone, with `head_unknown: true`. Postgres reads the whole chain.
- The plugin does not serve the external activity routes, such as
  `/activities/external/{token}/complete`. Call the `SqliteRuntime`
  methods instead (issue #2607).
- A stack lists no external handoff and has no worker id. A task stores no
  attempt start and no heartbeat time, so `start_to_close_deadline`,
  `heartbeat_deadline` and `last_heartbeat_at` are `null`. `scheduled_at`
  is the time that the next attempt may run, as on Postgres. For the same
  reason, an awaitable activity gets only its `schedule_to_close` deadline.
- A route that reads no query key refuses one: the schema, the interface,
  the run chain, the timeline, the awaitables and the stack.
- A timeline event written before #2489 has no time. It takes the time of
  the event before it, so it adds no duration.
- A row written before #2027 has no start time. A child, a tree node and a
  chain run then read `started_at` as the Unix epoch.
- `children` loads the descendants of the run to filter and sort them. One
  Harvest file holds the runs of one app, so the set is local.

## Auth, tokens, roles and audit

Auth works as on Postgres (#2469). The SQLite router goes through the same
admin-auth layer stack, `apply_admin_auth_layers`. Host auth from
`api_with_auth(path, mw)` runs outside it. The OIDC session boundary of
`api_with_oidc` runs outside that. So these options work as on Postgres:

- `enable_api_tokens`: scoped `hvst_` tokens, with `read`, `mutate` and
  `admin` scopes.
- `api_with_role_auth`: the read-only role (#776).
- `with_roles`: custom roles (#1978).
- `api_with_oidc`: OIDC login. The login keeps its principal in the
  autumn-web session.
- `with_authorizer`: the authorizer hook (#1803).
- `with_api_rate_limit`: the per-client rate limit (#1827).

The #1802 mutation gate refuses an anonymous mutating call outside `dev`.
`allow_unauthenticated_mutations()` opens it. `reuse_policy =
"terminate_if_running"` needs harvest admin access.

Tokens and audit rows live in the Harvest file, in `harvest_api_tokens` and
`harvest_audit_log`. A trigger refuses each `UPDATE` of an audit row.

- **Reads never wait.** The store reads on a read-only connection. In WAL
  mode a read never waits for a writer, so a token check never waits for
  the driver, which runs activity bodies inline.
- **Writes take turns.** The store writes on its own connection. Each
  store write is one short statement. Each driver transaction begins
  `IMMEDIATE`, so the driver and the store wait for each other on the busy
  timeout. Neither fails with `SQLITE_BUSY`.
- **Retention.** The store deletes audit rows older than
  `audit_retention_days` (90 by default), as on Postgres. It does so at boot
  and after each 256 audit writes. Zero, or a dry run, keeps every row.

Each of these writes one audit row:

- a token mint or a revoke;
- a deny of the token scope, a role, the authorizer or the tenant check;
- a sustained rate limit.

`GET /admin/audit` reads them, with the Postgres filters.

The tenant routes run the same stack, inside the `/tenants/{key}` nest. So
each layer sees the core route, and a read token reads a tenant. The
authorizer sees `{key}` as the tenant. A `x-harvest-tenant` header that
names another tenant is a `400`. With `api_with_oidc`, the session boundary
also runs inside the nest, so a tenant health route stays public.

To seed a first `admin` token offline, as `harvest token bootstrap` does on
Postgres, store its hash with `autumn_harvest_sqlite::SqliteAuthStore`:

```rust,ignore
let secret = autumn_harvest::api_token::mint_secret();
SqliteAuthStore::open("app.harvest.db")?.insert_token(&NewToken {
    id: uuid::Uuid::new_v4(),
    name: "bootstrap",
    token_hash: &autumn_harvest::api_token::hash_secret(&secret),
    scope: "admin",
    created_at: chrono::Utc::now(),
    expires_at: None,
    created_by: "bootstrap",
    tenant: None,
})?;
```

### Where auth differs

- **No tenant binding.** The SQLite store keeps no tenant on a run. So it
  cannot check a tenant-bound credential (#1977). A token mint with a
  `tenant` is a `400`. A request with a verified tenant, from a token or
  from `VerifiedTenant`, is a `501` on each non-public route. Both name the
  epic. Neither runs the request unchecked.
- **The workflow routes write no audit row.** On Postgres, a start, a
  signal, a cancel and the other mutations each write one. On SQLite,
  `GET /admin/audit` lists the token and deny rows only.
- The token and audit routes serve the control Harvest. No
  `/tenants/{key}/..` route serves them.

## What the SQLite backend refuses

This list covers embedded mode. Split and external mode take the Postgres
path, which honours every option. Each of these fails the build or the boot
and names its issue. A silent
no-op would let an app believe a protection or a feature is on.

| Option or mode | Issue |
| --- | --- |
| `decode_payloads_on_read`: this backend stores no codec envelope, so a read has nothing to decode | the epic |
| `synthetic_canary`, `agent_overlay_store` | the epic |
| `agent` with `sqlite_per_tenant()`: each tenant would share one agent memory | the epic: see [The durable agent](#the-durable-agent) |
| `RetentionConfig::tenant_overrides`, `summary`, `payload_orphan_grace_secs` | the epic: see [Retention](#retention) |
| A webhook or connector `queue`, and a `Starts` connector binding in `BrokerCoordinates` mode | the epic: see [MCP, webhooks and connectors](#mcp-webhooks-and-connectors) |
| `harvest.sqlite.path` or `sqlite_per_tenant()` with `harvest.mode = "split"` / `"external"` | [#2472](https://github.com/autumn-foundation/autumn-harvest/issues/2472): no effect in that mode |
| `sqlite_per_tenant()` in an app with no `[database.fleet]` | [#2472](https://github.com/autumn-foundation/autumn-harvest/issues/2472) |
| `harvest.worker_enabled = false` | not planned: the app process is the only writer. The `web` role does not stop the driver either ([#2477](https://github.com/autumn-foundation/autumn-harvest/issues/2477)) |
| DAGs | an engine non-goal of the SQLite backend (#2019) |
| `with_metrics_scrape` | the epic |

These options do nothing on SQLite, because they tune Postgres machinery that
this backend does not have: `worker(..)`, `load_shed`, `ramp_guard`,
`start_idempotency_window`, the durable-route limits, the status
thresholds and `harvest.outbox.claim_ttl_ms`.

## Tests

`autumn-harvest-plugin/tests/sqlite_plugin.rs` boots a real app on a temp
directory, with no Postgres and no Docker:

```bash
cargo test -p autumn-harvest-plugin --features sqlite,oidc,mcp,webhooks,connectors --test sqlite_plugin
cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_fleet
cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_agent
```

CI runs each suite in the `sqlite-plugin` job, with `oidc`, `mcp`, `webhooks` and
`connectors` on. One clippy step turns on `sqlite` alone, for the code that
those features turn off. The
`ops_parity` module covers schedules and retention. The `trigger_parity`
module covers MCP, webhooks and connectors, and runs only with those
features. The
fleet suite boots a tenant fleet. It covers each tenant's file and driver,
the tenant routes, a close, a reopen and a delete.
The agent suite runs the overlay read suite of the agent crate on the
plugin, as a third backend. It covers ops, approvals and overlay reads
through the handle.

`tests/sqlite_plugin_split.rs` runs a SQLite app on a Postgres Harvest
database in split and external mode. It starts a Postgres container, or
uses `HARVEST_TEST_DATABASE_URL`. CI runs it in the `sqlite-plugin-split`
job:

```bash
cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_split
```

The SQLite suite's `auth_parity` module runs the shared auth checks of
`tests/support/token_contract.rs`, `role_contract.rs` and
`oidc_contract.rs`. The Postgres suites `token_auth_integration`,
`custom_roles` and `oidc_login` run the same checks.

The SQLite suite's `wire_parity` and `read_parity` modules run the checks
of `tests/support/wire_contract.rs`. The Postgres suite of each route runs
the same check, for example `workflow_result_integration`,
`workflow_count_integration` and `lineage_tree_integration`. A change to the contract
changes that one file, and both backends then test it.
