# Split web and worker replicas with `AUTUMN_ROLE`

Harvest follows the Autumn process role. Set `AUTUMN_ROLE` on each replica.
Set no Harvest switch.

| `AUTUMN_ROLE` | Harvest worker | Harvest scheduler | Management API |
|---|---|---|---|
| `web` | off | off | on |
| `worker` | on | on | off: a `worker` replica serves no app routes |
| `combined` (the default) | on | on | on |

A `web` replica still starts workflows, signals runs and serves the
management API. It claims no task and fires no schedule. The `worker`
replicas do that work.

## Harvest capabilities

Harvest declares three Autumn plugin capabilities. Each one takes its
default from a built-in capability of the role:

| Capability | Default from | Runtimes |
|---|---|---|
| `harvest.worker` | `job_workers` | worker |
| `harvest.scheduler` | `scheduler` | scheduler |
| `harvest.maintenance` | `maintenance` | retention janitor, batch executor |

A named role can turn each one off, or keep it on when it turns the parent
off:

```toml
[roles.cron]
base = "worker"
capabilities = { "harvest.worker" = false }

[roles.flows]
base = "worker"
job_workers = false
capabilities = { "harvest.worker" = true }
```

`cron` runs the Harvest scheduler and Autumn job workers, but no Harvest
worker. `flows` runs the Harvest worker, but no Autumn job worker. A role
cannot turn on a capability that its base does not run, so a `web` base
runs none. `pin = []` turns off `job_workers`, so it also turns off
`harvest.worker` unless the role sets it.

## Rules

- The role is the primary knob. Set `AUTUMN_ROLE` and, for a named role,
  its `capabilities`.
- `harvest.worker_enabled` and `harvest.scheduler_enabled` are overrides.
  They are unset by default, and an unset switch follows the role.
- An explicit value wins. Set it in `[harvest]` or with
  `AUTUMN_HARVEST__WORKER_ENABLED` and `AUTUMN_HARVEST__SCHEDULER_ENABLED`.
- An explicit value against the role logs one `warn!` per switch. The
  warning names the switch, its value, the role and the capability.
- An explicit `true` on a role that does not place a task also gets the
  Autumn warning for a forced task. `worker_enabled = true` on `web`
  warns for `harvest.worker`, `harvest.batch` and `harvest.queues`.
  `run_on = "all"` on `web` warns for `harvest.retention`.
- At startup, Harvest logs one line with each value and its source:
  `explicit`, `role` or `default`. The SQLite backend logs the source
  `sqlite`.
- A runtime with no Autumn `AppState` has no role. A standalone
  `HarvestRunner` is one example. Both switches then stay on.
- The SQLite backend always runs its driver. A role that turns off
  `harvest.worker`, `web` included, does not stop it, and Harvest logs a
  warning. Set `harvest.worker_enabled = true` to
  remove the warning. The SQLite backend has no schedules, so it ignores
  `harvest.scheduler_enabled`. See [`docs/sqlite-plugin.md`](../sqlite-plugin.md).

A split role needs a jobs backend that both process groups share, such as
`postgres` or `redis`. Autumn refuses the boot otherwise. Set it once in the
shared `autumn.toml`:

```toml
[jobs]
backend = "postgres"
```

## Background runtimes

Each role runs these Harvest runtimes. No switch is set, and retention
is on.

| Runtime | Autumn task | `web` | `worker`, `combined`, no role |
|---|---|---|---|
| Worker | `harvest.worker` | off | on |
| Scheduler | `harvest.scheduler` | off | on |
| Batch executor | `harvest.batch` | off | on |
| Retention janitor | `harvest.retention` | off | on |
| Dispatch-metrics sampler | none | on | off: the worker samples |
| Post-commit notify sender | none | on | on |
| Codec key refresh | none | on | off: the worker refreshes |

The test `each_role_places_each_runtime` in `runner.rs` pins this table.

Autumn starts each task after the startup hooks, on a role that places
it. `GET /actuator/tasks` lists each task under `background_tasks`, with
its capability, the roles that run it and its state on this process.
`autumn graph` lists the same tasks as `background_task` nodes. A fifth
task, `harvest.queues`, runs nothing. It declares the Harvest queues to
Autumn. See [Coverage](#coverage).

- **Shutdown.** Autumn cancels each task when the listener stops, then
  waits for at most half of the remaining `server.shutdown_timeout_secs`.
  Each Harvest loop runs on its own Tokio task. So when Autumn aborts a
  slow task, the loop still finishes its drain. The Harvest shutdown hook
  then waits for each loop. With the defaults, Autumn waits 15 s, and the
  worker drains for up to `WorkerConfig::shutdown_timeout` (25 s). Set
  `server.shutdown_timeout_secs` to at least twice that value, and keep
  `terminationGracePeriodSeconds` above it.
- **A combined process stops its worker first.** The worker stops when
  the listener stops, before in-flight HTTP requests end. A request that
  waits for a run on this process, such as a durable route, then needs
  another worker to finish the run.
- **Autumn one-off tasks run no Harvest loop.** `autumn task` runs the
  startup hooks, but no background task. Harvest logs the runtimes that
  it did not start.
- **A standalone runner**, `HarvestEmbedding` included, has no Autumn
  host. It spawns each runtime itself, as before.
- **Retention follows the role.** That is the default,
  `run_on = "scheduler"`. An explicit `scheduler_enabled` moves retention
  with it. With none, the `harvest.maintenance` capability decides.
- **`run_on = "all"` runs retention on each process**, as in 0.9.0.
  Row leases keep concurrent janitors correct. On a `web` role, Autumn
  logs its forced-task warning for `harvest.retention` at each boot. Set it in
  `autumn.toml`, or with `AUTUMN_HARVEST_RETENTION__RUN_ON=all`:

  ```toml
  [harvest.retention]
  run_on = "all"
  ```

  An unknown value stops the boot.
- **The notify sender runs on each process.** A `web` replica writes rows
  too, so it sends the notifications that wake the waiters.
- **The batch executor follows the role.** An explicit `worker_enabled`
  moves it with the worker. With none, `harvest.maintenance` decides.
- **The dispatch-metrics sampler runs only with no worker.** A worker
  runs its own copy. Two copies would emit each gauge twice.
- A process with retention on but not placed logs one `info!` at
  startup. The line names `harvest.maintenance`, `scheduler_enabled` and
  the fix.
- A typo in `run_on` stops the boot on the SQLite backend too, though
  SQLite ignores the value.
- The SQLite backend runs retention in its driver on every role. It
  ignores `run_on`.
- A standalone runner, `HarvestEmbedding` included, reads no
  `[harvest.retention]`. It takes `run_on` from the `HarvestRuntimeConfig`
  that its caller passes. With no role and the scheduler off, it logs a
  `warn!`, because its retention may run nowhere.

**The retention API on a `web` replica.** `GET /admin/retention` and
`POST /admin/retention/run-now` read the janitor of the process that
serves them. A `worker` replica serves no app routes, so on a split fleet
the API reaches no janitor by default:

- `GET /admin/retention` shows the config, no shard results and
  `placed_elsewhere: true`.
- `POST /admin/retention/run-now`, and `harvest retention run-now`,
  return 503. The message names `run_on = "all"`.

To trigger a sweep from the API, set `run_on = "all"` on the `web`
replicas. Otherwise the next tick on a `worker` replica runs the sweep.
Read the retention metrics of the `worker` replicas for its results.

## Worker queues

Code declares the full set of queues with `WorkerConfig::with_queues`.
Config can narrow the queues that the worker of one process polls. Config
cannot add a queue.

```toml
[harvest.worker]
queues = ["priority"]
```

Or set `AUTUMN_HARVEST__WORKER__QUEUES=priority`. Separate names with a
comma. The variable wins over the file.

- **An undeclared queue stops the boot.** The error names it and the
  declared queues. A name matches exactly, so `Priority` is not
  `priority`. A typo cannot make a tier that polls nothing.
- **An empty list means "not set"**, as for `AUTUMN_JOBS__PIN`. An empty
  variable clears a list from the file. An empty list in a profile file
  clears a list from `autumn.toml`.
- **The worker polls in code order**, not in config order.
- **The management API keeps the declared list.** A start with no queue
  goes to the first declared queue on each tier.
- `[harvest.worker]` refuses an unknown key. `queue = [...]` stops the boot.
- The variable follows the Autumn form `AUTUMN_<section>__<key>`. It does
  not follow the form of other Harvest sections, such as
  `AUTUMN_HARVEST_RETENTION__RUN_ON`. `AUTUMN_HARVEST_WORKER__QUEUES` stops the boot and names the
  right variable.
- **Deploy order.** Set the list only after each process runs
  0.10.0 or later. An older process ignores `[harvest.worker]` and polls
  each queue.

### The role pin

With no explicit list, Harvest reads the `pin` of a named Autumn role
(`autumn-web` 0.10 and later):

```toml
[roles.priority]
base = "worker"
pin = ["priority", "emails"]
```

Autumn job queues and Harvest queues share the pin. Harvest polls each
declared queue that the pin names, and ignores the other names. A pin that
names no declared queue changes nothing. Harvest then polls each declared
queue. A process that runs the worker logs one `warn!`, which names the
pin and the fix. An explicit list always wins over the pin.

Harvest reads the `pin` of a `[roles.<name>]` preset only. It does not
read `jobs.pin` or `AUTUMN_JOBS__PIN`. A built-in role has no pin.

A preset places each Harvest runtime through the Harvest capabilities.
See [Harvest capabilities](#harvest-capabilities). The pin narrows the
queues of a worker that the role places. It does not stop the worker.

### Route groups

`autumn-web` 0.10 and later serve only the route groups that a preset names in
`mount`. Each Harvest route is in the group `harvest`: the management API,
the UI, the webhook receivers and the MCP routes. A preset that mounts
groups must name `harvest`. Otherwise its replicas serve no Harvest route,
and the Harvest readiness probe returns 404.

```toml
[roles.site]
base = "web"
mount = ["app", "harvest"]
```

| Explicit list | Role pin | The worker polls |
|---|---|---|
| set | any | the explicit list |
| not set | names a declared queue | the declared queues in the pin |
| not set | names no declared queue | each declared queue, with a `warn!` |
| not set | none | each declared queue |

`formal/verus/src/worker_queues.rs` proves the source of each row. The
proptests in `worker_queues.rs` check the polled list.

### Coverage

Each queue needs a tier that polls it. Otherwise a run on it waits.

- At startup, Harvest logs one line with the declared queues, the polled
  queues and the source: `code`, `explicit` or `role`.
- `GET /admin/config` reports `worker.queues` (polled),
  `worker.declared_queues` and `worker.queue_source`.
- `GET /admin/queue-coverage` lists `unpolled_declared_queues`: each
  declared queue with no live worker on a writable shard. A drained shard
  takes no new run, so it does not count. `null` means "unknown": no
  runtime is installed, or a writable shard gave no answer.
- `harvest queue coverage --strict` exits `2` when that list is not empty,
  `null` or missing. An older server omits it, so the gate fails closed.
- The task `harvest.queues` declares each queue to Autumn. So
  `autumn jobs manifest` lists them, and the `autumn doctor` coverage
  check includes them. A role `pin` that names only Harvest queues does
  not log the Autumn "pin matches no work" error. The task runs on a role
  that runs the Harvest worker, runs Autumn job workers and drains a
  Harvest queue. A role with no Autumn job worker, such as `flows` above,
  still runs the Harvest worker, but Autumn counts no Harvest queue there.
- Autumn applies the role `pin`, or `jobs.pin`, to `harvest.queues`.
  Harvest does not read `jobs.pin`. So a `jobs.pin` that names no Harvest
  queue hides the Harvest queues from Autumn, though the worker still
  polls them.

With the `webhooks` feature, the built-in `webhooks` queue is a declared
queue too. Narrow it out only when another tier polls it.

The SQLite backend refuses a list. Its one driver runs each queue. With a
SQLite profile in development and a Postgres profile in production, put
the list in the Postgres profile file or in the variable. Do not put it in
an `autumn.toml` that the SQLite profile also reads. On SQLite, a role pin
narrows nothing, and Harvest logs one `info!`.

A standalone `HarvestEmbedding` reads `[harvest.worker]` and the variable.
A plain `HarvestRunner::start` reads the `HarvestRuntimeConfig` that its
caller passes, and the role pin of an app state in its resources.

## Fly.io

Each process group runs the same image. Only `AUTUMN_ROLE` differs in the
app config. A `fly.toml` has no `[env]` table per process group, so each
command sets `AUTUMN_ROLE`. A command in `[processes]` replaces the image
`CMD`, not its `ENTRYPOINT`. Use an image with no `ENTRYPOINT`, or one that
runs its arguments.

```toml
# fly.toml
app = "orders"

[processes]
web = "env AUTUMN_ROLE=web /app/orders"
worker = "env AUTUMN_ROLE=worker /app/orders"

[http_service]
internal_port = 3000
processes = ["web"]

[[http_service.checks]]
method = "GET"
path = "/api/harvest/health/ready"
interval = "10s"
timeout = "2s"

[checks.worker_ready]
type = "http"
port = 3000
path = "/ready"
interval = "10s"
timeout = "2s"
processes = ["worker"]

[[vm]]
processes = ["web"]
size = "shared-cpu-1x"

[[vm]]
processes = ["worker"]
size = "shared-cpu-2x"
```

Scale each group on its own:

```bash
fly scale count web=2 worker=4
```

An image with no `env` binary, such as a distroless image, needs a small
entry script that exports `AUTUMN_ROLE` and then runs the app.

## Kubernetes

Two Deployments run the same image. Only `AUTUMN_ROLE` differs in the app
config.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-web
spec:
  replicas: 2
  selector:
    matchLabels: { app: orders, role: web }
  template:
    metadata:
      labels: { app: orders, role: web }
    spec:
      containers:
        - name: orders
          image: ghcr.io/example/orders:1.4.0
          env:
            - name: AUTUMN_ROLE
              value: web
          ports:
            - containerPort: 3000
          livenessProbe:
            httpGet: { path: /api/harvest/health/live, port: 3000 }
          readinessProbe:
            httpGet: { path: /api/harvest/health/ready, port: 3000 }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-worker
spec:
  replicas: 4
  selector:
    matchLabels: { app: orders, role: worker }
  template:
    metadata:
      labels: { app: orders, role: worker }
    spec:
      # A worker drains in-flight tasks for up to its shutdown timeout.
      # Keep this period above server.shutdown_timeout_secs, which is at
      # least twice that timeout. See kubernetes-probes.md.
      terminationGracePeriodSeconds: 60
      containers:
        - name: orders
          image: ghcr.io/example/orders:1.4.0
          env:
            - name: AUTUMN_ROLE
              value: worker
          ports:
            - containerPort: 3000
          livenessProbe:
            httpGet: { path: /live, port: 3000 }
          readinessProbe:
            httpGet: { path: /ready, port: 3000 }
```

Route the Service to the `web` pods only. A `worker` pod serves only the
Autumn probes and the actuator.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: orders
spec:
  selector: { app: orders, role: web }
  ports:
    - port: 80
      targetPort: 3000
```

A `web` pod uses the Harvest probes, which also check the Harvest
database. The examples mount Harvest at `/api/harvest`. A `worker` pod
serves no Harvest route, so it uses the Autumn probes `/live` and `/ready`.
See [`kubernetes-probes.md`](kubernetes-probes.md).

## Upgrade note

Harvest 0.11.0 needs `autumn-web` 0.11, and Autumn places each Harvest
runtime. Read [`autumn-web` 0.11](../upgrading/autumn-web-0.11.md) and the
[0.11.0 upgrade guide](../upgrading/0.11.0.md) first.
A named role now places Harvest through its capabilities.

The notes below are for the upgrade to 0.10.0.

Before 0.10.0, a `web` replica ran the Harvest worker and scheduler.
It no longer does. A fleet with only `web` replicas then claims no task.
Do one of these before the upgrade:

- Add `worker` replicas.
- Set `AUTUMN_HARVEST__WORKER_ENABLED=true` and
  `AUTUMN_HARVEST__SCHEDULER_ENABLED=true` on the `web` replicas. Harvest
  logs a warning for each override.

A fleet that sets `AUTUMN_HARVEST__WORKER_ENABLED=false` on its API
replicas, with no `AUTUMN_ROLE`, keeps working. Its role is `combined`, so
Harvest now logs a warning for that override. To remove the warning, set
`AUTUMN_ROLE=web` and remove the variable. Read these effects first:

- The Harvest scheduler also stops on those replicas.
- Autumn's own `#[job]` workers and cron tasks stop on those replicas too.
- Autumn refuses the boot with no shared jobs backend. See [Rules](#rules).

0.10.0 moves the retention janitor off `web` replicas. A fleet with
`worker` replicas keeps retention. A fleet that turns the scheduler off on
every process runs no retention. Set `run_on = "all"` on one group, or
turn the scheduler on. See [Background runtimes](#background-runtimes).
