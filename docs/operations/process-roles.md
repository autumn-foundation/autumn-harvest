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
  `explicit`, `role` or `default`.
- A runtime with no Autumn `AppState` has no role. A standalone
  `HarvestRunner` is one example. Both switches then stay on.
- The SQLite backend always runs its driver. The `web` role does not stop
  it, and Harvest logs a warning. See [`docs/sqlite-plugin.md`](../sqlite-plugin.md).

A split role needs a jobs backend that both process groups share. Autumn
refuses the boot otherwise. Set it once in the shared `autumn.toml`:

```toml
[jobs]
backend = "postgres"
```

## Fly.io

Each process group runs the same image. Only `AUTUMN_ROLE` differs. Fly
sets no environment per process group, so the command sets it.

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

Two Deployments run the same image. Only `AUTUMN_ROLE` differs.

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
      # A worker finishes its in-flight activities before it stops.
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
replicas keeps working. Replace that variable with `AUTUMN_ROLE=web`.
