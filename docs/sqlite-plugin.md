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

## Turn it on

```toml
[dependencies]
autumn-web = { version = "0.9", features = ["sqlite"] }
autumn-harvest = { version = "0.9", default-features = false }
autumn-harvest-plugin = { version = "0.9", features = ["sqlite"] }
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
its file.

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
  `ctx.state::<AppState>()`. The SQLite outbox is
  [#2470](https://github.com/autumn-foundation/autumn-harvest/issues/2470).
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
- A fleet restore rolls back the tenant database, not its Harvest file.
- Fleet replication does not ship Harvest files.

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
| `GET`/`POST /admin/tokens`, `DELETE /admin/tokens/{id}` | the Postgres handlers. See [Auth](#auth-tokens-roles-and-audit) |
| `GET /admin/audit` | the Postgres handler and filters |

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

Every other management route answers `501` and links the epic.

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
`harvest_audit_log`. A trigger refuses each `UPDATE` of an audit row. The
store has its own connection to the file. So a token check never waits for
the driver, which runs activity bodies inline.

These write one audit row each: a token mint and a revoke, and each deny of
the token scope, a role, the authorizer, the tenant check and a sustained
rate limit. `GET /admin/audit` reads them, with the Postgres filters.

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
| Vantage, `decode_payloads_on_read`, the other read routes, declarative `queries![..]` (a query that the workflow registers with `ctx.register_query` works) | [#2468](https://github.com/autumn-foundation/autumn-harvest/issues/2468) |
| The transactional outbox | [#2470](https://github.com/autumn-foundation/autumn-harvest/issues/2470) |
| Workflow schedules, `retention`, `mcp_tools`, `mcp_tasks`, `webhooks`, `connector`, `synthetic_canary`, `agent_overlay_store` | [#2471](https://github.com/autumn-foundation/autumn-harvest/issues/2471) |
| `harvest.sqlite.path` or `sqlite_per_tenant()` with `harvest.mode = "split"` / `"external"` | [#2472](https://github.com/autumn-foundation/autumn-harvest/issues/2472): no effect in that mode |
| `sqlite_per_tenant()` in an app with no `[database.fleet]` | [#2472](https://github.com/autumn-foundation/autumn-harvest/issues/2472) |
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
cargo test -p autumn-harvest-plugin --features sqlite --test sqlite_plugin_fleet
```

CI runs both in the `sqlite-plugin` job, with the `oidc` feature on. The
fleet suite boots a tenant fleet. It covers each tenant's file and driver,
the tenant routes, a close, a reopen and a delete.

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

The SQLite suite's `wire_parity` module runs the checks of
`tests/support/wire_contract.rs`. The Postgres suites
`workflow_result_integration`, `workflow_history_pagination_integration`
and `by_id_integration` run the same checks. A change to the contract
changes that one file, and both backends then test it.
