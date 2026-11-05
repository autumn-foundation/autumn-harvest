# Split web and worker replicas with `AUTUMN_ROLE` (issue #2477)

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

## Rules

- `harvest.worker_enabled` and `harvest.scheduler_enabled` are unset by
  default. An unset switch follows the role.
- An explicit value wins. Set it in `[harvest]` or with
  `AUTUMN_HARVEST__WORKER_ENABLED` and `AUTUMN_HARVEST__SCHEDULER_ENABLED`.
- An explicit value against the role logs one `warn!` per switch. The
  warning names the switch, its value and the role.
- At startup, Harvest logs one line with each value and its source:
  `explicit`, `role` or `default`. The SQLite backend logs the source
  `sqlite`.
- A runtime with no Autumn `AppState` has no role. A standalone
  `HarvestRunner` is one example. Both switches then stay on.
- The SQLite backend always runs its driver. The `web` role does not stop
  it, and Harvest logs a warning. Set `harvest.worker_enabled = true` to
  remove the warning. The SQLite backend has no schedules, so it ignores
  `harvest.scheduler_enabled`. See [`docs/sqlite-plugin.md`](../sqlite-plugin.md).

A split role needs a jobs backend that both process groups share, such as
`postgres` or `redis`. Autumn refuses the boot otherwise. Set it once in the
shared `autumn.toml`:

```toml
[jobs]
backend = "postgres"
```

## Background runtimes (issue #2478)

Each role runs these Harvest runtimes. No switch is set, and retention
is on.

| Runtime | `web` | `worker`, `combined`, no role |
|---|---|---|
| Worker | off | on |
| Scheduler | off | on |
| Batch executor | off | on |
| Retention janitor | off | on |
| Dispatch-metrics sampler | on | off: the worker samples |
| Post-commit notify sender | on | on |
| Codec key refresh | on | off: the worker refreshes |

The test `each_role_places_each_runtime` in `runner.rs` pins this table.

- **Retention runs where the scheduler runs.** That is the default,
  `run_on = "scheduler"`. An explicit `scheduler_enabled` moves retention
  with it.
- **`run_on = "all"` runs retention on each process**, as before issue
  #2478. Row leases keep concurrent janitors correct. Set it in
  `autumn.toml`, or with `AUTUMN_HARVEST_RETENTION__RUN_ON=all`:

  ```toml
  [harvest.retention]
  run_on = "all"
  ```

  An unknown value stops the boot.
- **The notify sender runs on each process.** A `web` replica writes rows
  too, so it sends the notifications that wake the waiters.
- **The dispatch-metrics sampler runs only with no worker.** A worker
  runs its own copy. Two copies would emit each gauge twice.
- A process with retention on but not placed logs one `info!` at
  startup. The line names `run_on` and the fix.
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
      # Keep this period longer. See kubernetes-probes.md.
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

Before issue #2477, a `web` replica ran the Harvest worker and scheduler.
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

Issue #2478 moves the retention janitor off `web` replicas. A fleet with
`worker` replicas keeps retention. A fleet that turns the scheduler off on
every process runs no retention. Set `run_on = "all"` on one group, or
turn the scheduler on. See [Background runtimes](#background-runtimes-issue-2478).
