# Upgrading: `autumn-web` 0.11

`autumn-harvest-plugin` now needs `autumn-web` 0.11. That release adds the
role-aware background-task API (autumn#3545), and Harvest places its
runtimes through it. Read [Process roles](../operations/process-roles.md)
for the full model. This page lists what an app must change.

---

## 1. Bump `autumn-web` with Harvest

The public API of the plugin holds `autumn-web` types, so Cargo treats 0.10
and 0.11 as different crates.

1. Bump your own `autumn-web` entry to `"0.11"` in the same change.
2. Check that `cargo tree -i autumn-web` shows one version.

An app on `autumn-web` 0.10 does not compile: `HarvestPlugin` implements
the 0.11 `Plugin` trait. `HarvestPlugin` also declares the range `0.11`.

`autumn-web` 0.11 also moves these crates. Bump them in your own manifest
if you name them:

| Crate | Before | After |
|---|---|---|
| `diesel` | 2.3 | 2.4 |
| `diesel-async` | 0.9 | 0.10 |
| `libsqlite3-sys` | 0.38 | 0.40 |
| `rusqlite` | 0.40 | 0.42 |

`autumn-web` 0.11 uses `jiff` for dates and times. The plugin no longer
depends on `chrono`, under any feature.

## 2. Code that the bump can break

- **`diesel` 2.4.** A custom query type needs
  `diesel::query_dsl::RunQueryDslSupport` before `diesel-async` runs it. A
  function that returns `impl AsChangeset` for an `UPDATE ... WHERE` can
  fail to compile. Return the concrete type, or build the changeset inline.
- **`rusqlite` 0.42.** `Connection::progress_handler`, `authorizer` and
  `trace_v2` take `&mut self`. `trace_v2` returns a `Result`.
- **`ProcessSwitches::resolve`** and
  **`HarvestRuntimeConfig::process_switches`** take
  `Option<&autumn_web::config::Role>`, not a `ProcessRole`. Pass
  `state.role()`. `ProcessSwitches` is no longer `Copy`.

## 3. Behavior changes

- **The Autumn drain stops the Harvest loops.** The worker, the
  scheduler, the retention janitor and the batch executor run as Autumn
  background tasks. Autumn cancels them when the listener stops, before
  the `on_shutdown` hooks. It waits for at most half of the remaining
  `server.shutdown_timeout_secs`, 15 s with the defaults. Each loop runs
  on its own Tokio task, so an abort of the Autumn task does not cut its
  drain short. The Harvest shutdown hook waits for each loop. Set
  `server.shutdown_timeout_secs` to at least twice
  `WorkerConfig::shutdown_timeout` (25 s by default).
- **A combined process stops its worker when the listener stops.** A
  request that waits for a run on this process, such as a durable route,
  needs another worker to finish the run during the drain.
- **Autumn one-off tasks run no Harvest loop.** `autumn task` runs the
  startup hooks, but no background task.
- **Named roles place Harvest.** Before, Harvest read only the base of a
  `[roles.<name>]` preset and logged a warning. Now it reads the Harvest
  capabilities of the preset. A preset with `job_workers = false` or
  `pin = []` stops the Harvest worker. A preset with `maintenance = false`
  stops the retention janitor and the batch executor. Set
  `capabilities = { "harvest.worker" = true }` to keep the worker.
- **The batch executor follows `harvest.maintenance`.** Before, it ran with
  the worker. An explicit `worker_enabled` still moves it. A built-in role
  runs it where it ran before.
- **`run_on = "scheduler"`** follows an explicit `scheduler_enabled`. With
  none, the `harvest.maintenance` capability decides. A built-in role runs
  the janitor where it ran before. Verus proves it.
- **`run_on = "all"` warns on a `web` replica.** It is an explicit switch
  on a role that does not place the janitor, so Autumn logs its warning
  for a forced task.
- **New `/actuator/tasks` entries.** `harvest.worker`,
  `harvest.scheduler`, `harvest.retention`, `harvest.batch` and
  `harvest.queues` appear under `background_tasks`. `autumn graph` lists
  them too.
- **`autumn doctor` sees the Harvest queues.** `autumn jobs manifest`
  lists them, through `harvest.queues`.

A standalone `HarvestRunner` or `HarvestEmbedding` has no Autumn host. It
spawns its runtimes as before.

## 4. Deploy order

The runtimes of 0.10 and 0.11 share the database as before. Nothing in
the schema changes. A rolling deploy needs no special order. Add the
Harvest capabilities to a `[roles.<name>]` preset only after each process
runs 0.11. `autumn-web` 0.10 refuses the `capabilities` key, so an older
process does not boot with it.
