# Design — Issue #2472: split mode and per-tenant Harvest on a SQLite app

Part of epic [#2466](https://github.com/autumn-foundation/autumn-harvest/issues/2466).
Slice 1 ([#2467](https://github.com/autumn-foundation/autumn-harvest/issues/2467))
refused `split` and `external` at boot, and ran one Harvest file per app.
This issue adds two things:

1. **Split and external mode.** A SQLite app keeps its Harvest storage in a
   Postgres database.
2. **Per-tenant Harvest.** A SQLite fleet app runs one Harvest file and one
   driver for each open tenant database.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `HarvestPlugin::build` takes the SQLite path for every mode under the
  `sqlite` feature. `sqlite::mode_refusal` panics on `split` and `external`.
- The Postgres path compiles under `sqlite`. Two functions in `state.rs`
  hold every app-pool access: `app_postgres_pool` and
  `create_postgres_pool`. Under `sqlite` the first returns `None` and the
  second refuses.
- In split mode the Postgres runtime builds its own pool from
  `harvest.database.url`. Only the outbox relay and `AppDbPool` read the app
  pool. Both skip a `None` pool already.
- `register_plugin_migrations` registers the outbox set on the app database
  in every mode. That set is Postgres SQL. A SQLite app database cannot
  apply it.
- Under `sqlite`, `autumn_web::db::create_pool` builds SQLite pools only.
  Its Postgres TLS helper is private. `autumn_harvest::pg_tls::connect`
  opens a Postgres connection with the DSN's `sslmode`, and the plugin
  already depends on it.
- `autumn_web::migrate::run_pending` needs the `postgres` feature of
  `autumn-web`. That feature stays on under `sqlite`, so the split path can
  migrate the Harvest database.
- `autumn-web` 0.9 has `FleetLifecycle`: `on_open`, `on_close`, `release`,
  `on_delete`, `restore_missing`. `DatabaseFleet::add_lifecycle` installs
  one. A database that is open before the install sees no `on_open`.
- `FleetDatabase::register_worker` ties a task to one database. The fleet
  cancels its token and waits for it (bounded) before the pool closes. A
  delete or a restore aborts it at once.
- A `FleetDatabase::lease` keeps a database open while a holder lives.
- Production `run` starts the fleet runtime before the startup hooks.
  `TestApp` runs no fleet runtime.
- A tenant id allows only `[a-z0-9_-]`. A file named `acme.harvest.db`
  never parses as a tenant, so `DatabaseFleet::list` skips it.
- `SqliteRuntime` runs every activity inline on the driver task. It spawns
  no task.
- `ActivityInfo` is not `Clone`. Registration takes it by reference.
- The fleet sweeper wakes a closed database for outbox, hooks and
  backfill work only. `PendingKind` is a closed enum.

### 0.2 Brainstorm — how does a SQLite app reach a Postgres Harvest database?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Run the Postgres plugin path in split and external mode. | **Adopted.** It already builds its own pool, migrates the Harvest database and serves the whole API. |
| B2 | Port each Postgres surface to a second client type. | Rejected. It duplicates the Postgres plugin. |
| B3 | Build the Harvest pool through `autumn-web`. | Rejected. Under `sqlite` it builds SQLite pools only. |
| B4 | Build the pool with `diesel-async` and `pg_tls::connect`. | **Adopted.** The DSN keeps its `sslmode` meaning. |
| B5 | Register the outbox set on the SQLite app database. | Rejected. The set is Postgres SQL. The SQLite outbox is #2470. |
| B6 | Skip the outbox set under `sqlite`, and leave the relay off. | **Adopted.** No Postgres SQL reaches the app database. |

### 0.3 Brainstorm — how does a fleet app get one Harvest per tenant?

| # | Idea | Verdict |
|---|------|---------|
| T1 | Harvest tables inside each tenant database. | Rejected. `SqliteRuntime` holds an exclusive writer lock on its file. It needs autumn#3536 to scope a migration set too. |
| T2 | One Harvest file next to each tenant file. | **Adopted.** It follows the slice 1 rule. A copy of a tenant directory carries its history. |
| T3 | All tenant Harvest files in one separate directory. | Rejected. It splits a tenant's data over two trees. |
| T4 | A `FleetLifecycle` that opens a driver in `on_open`. | **Adopted.** The fleet already decides when a tenant is open. |
| T5 | Open every tenant at boot. | Rejected. It defeats `max_open`. |
| T6 | Hold a lease while a tenant has live runs. | Rejected. A long timer would pin the tenant open past `max_open`. |
| T7 | Register the driver task with `register_worker`. | **Adopted.** A close stops it before the pool goes. A delete aborts it. |
| T8 | Give each tenant's activities a state value with its key. | Rejected. Shared state values are not `Clone`. |
| T9 | A task-local tenant key, set around the driver loop. | **Adopted.** Activities run inline on that task. |
| T10 | Replace the control Harvest in per-tenant mode. | Rejected. Platform workflows, such as a tenant purge, need it. |
| T11 | Tenant routes under `{api}/tenants/{key}/..`. | **Adopted.** The same router and the same auth gate serve them. |

### 0.4 Reverse brainstorm — how could this go wrong?

| # | How to make it fail | Guard |
|---|------|------|
| R1 | Postgres outbox SQL runs on the SQLite app database. | The outbox set is not registered under `sqlite`. The split test boots on `dev`, which applies every registered set. |
| R2 | The split path silently reads the SQLite app pool as Postgres. | `app_postgres_pool` is `None` under `sqlite`. The relay and `AppDbPool` stay off. A unit test checks it. |
| R3 | `harvest.sqlite.path` is set with `mode = "split"` and silently ignored. | The build refuses it and names the conflict. |
| R4 | `sqlite_per_tenant()` with `mode = "split"`. | The build refuses it. |
| R5 | `sqlite_per_tenant()` in an app with no fleet. | The boot refuses it. |
| R6 | A tenant opens twice, so a second runtime hits the writer lock. | The registry marks a tenant `Starting` before the open. A second start returns at once. |
| R7 | A tenant opened before the startup hook gets no driver. | The hook starts a driver for each key in `open_keys()`. |
| R8 | A closed tenant keeps a driver and its writer lock. | The driver watches the worker token. `on_close` drops the handle. |
| R9 | A deleted tenant keeps its workflow history. | `on_delete` removes the Harvest file and its sidecars. |
| R10 | The registry handle keeps the tenant open forever. | The registry copy holds no lease. Only a handle given to a caller holds one. |
| R11 | A tenant route reaches another tenant. | The route resolves the key with the app's own `key_for`. One handle serves one request. |
| R12 | A tenant route skips the mutation gate. | The tenant routes sit inside the gated router. A test checks a `401`. |

### 0.5 Six thinking hats

- **White (facts).** See 0.1. The Postgres path needs a pool, a migration
  run and no app pool. The fleet gives open, close and delete hooks.
- **Red (feel).** Two Harvest kinds in one app can confuse. The handle
  names (`HarvestSqlite`, `HarvestFleet`) keep them apart.
- **Black (risk).** A closed tenant runs no timers until it opens again.
  A restore rolls back the tenant file, not its Harvest file. Replication
  does not ship Harvest files. Each is a documented limit (section 4).
- **Yellow (value).** A SQLite app can scale out its workflows on Postgres
  with no code change. A fleet app isolates each tenant's history, and a
  tenant delete erases it.
- **Green (ideas).** A Harvest `PendingKind` in `autumn-web` would let the
  sweeper wake a tenant with a due timer. That is a follow-up.
- **Blue (process).** RED tests first for each decision, then the code,
  then a refactor with the gates on. Part A, then part B.

---

## 1. Decision

### 1.1 Split and external mode

1. Under `sqlite`, `HarvestPlugin::build` reads `harvest.mode`. `embedded`
   takes the SQLite path. `split` and `external` take the Postgres path.
2. `create_postgres_pool` builds a `diesel-async` pool under `sqlite`. Each
   connection opens with `pg_tls::connect`.
3. `register_plugin_migrations` registers no outbox set under `sqlite`.
4. The build refuses `harvest.sqlite.path` and `sqlite_per_tenant()` in
   split and external mode. A path set with `HarvestPlugin::sqlite_path`
   only logs a warning: it is a default for embedded mode.

### 1.2 Per-tenant Harvest in a fleet

1. `HarvestPlugin::sqlite_per_tenant()` turns it on. The control Harvest
   keeps running.
2. A tenant's Harvest file sits next to its database: `acme.db` gives
   `acme.harvest.db`.
3. The plugin installs a `FleetLifecycle` at startup:
   - `on_open` opens the file and starts a driver. It registers the driver
     task with `register_worker`.
   - `on_close` drops the tenant's handle.
   - `on_delete` removes the Harvest file, `-wal`, `-shm` and `-journal`.
     It keeps the `.lock` file, as the lock module requires.
4. The startup hook also starts a driver for each tenant that is open
   already.
5. `HarvestFleet` is an `AppState` extension and an extractor. `open_for`
   and `open` return a `HarvestSqlite` that holds the tenant's lease.
6. `HarvestTenant::current()` gives the tenant key inside an activity.
7. `.api(path)` also serves `{path}/tenants/{key}/..`. `{key}` is the
   app's routing key. The routes are the slice 1 routes.

### 1.3 The dependencies

- **autumn#3536** (scope a migration set) is not needed. Harvest has its
  own file and applies its own schema. No set goes into a tenant database.
- **autumn#3537** (local restore) would help. Today a restore rolls back
  the tenant file and keeps the current Harvest file.

## 2. Acceptance criteria

| # | Criterion |
|---|---|
| AC1 | A SQLite app with `harvest.mode = "split"` or `"external"` boots and runs a workflow on its Postgres Harvest database. |
| AC2 | The outbox relay and the app-pool paths stay off the SQLite app pool. No Postgres migration reaches the app database. |
| AC3 | A CI lane with Docker runs the split test. |
| AC4 | A per-tenant app runs one Harvest file and one driver per open tenant database. |
| AC5 | A `FleetLifecycle` starts and stops each tenant driver. A delete removes the tenant's Harvest file. |
| AC6 | Each option that conflicts with the mode fails loud and names the conflict. |

## 3. Tests

- `sqlite::tests` and `plugin::tests`: mode routing, refusals, the pool
  builder, the file name and the registry.
- `tests/sqlite_plugin_fleet.rs`: a fleet app with two tenants, the
  lifecycle, the tenant routes and a delete. No Docker.
- `tests/sqlite_plugin_split.rs`: a SQLite app on a Postgres Harvest
  database in a container. CI runs it in the `sqlite-plugin-split` job.

## 4. Limits

- A closed tenant runs no timer, retry or activity until it opens again.
  The next open resumes its runs.
- A fleet restore does not roll back the tenant's Harvest file.
- Fleet replication does not ship Harvest files.
- The transactional outbox is not available on a SQLite app (#2470).
