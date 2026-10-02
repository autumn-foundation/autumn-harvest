# standalone-runner

This example shows the out-of-the-box non-`HarvestPlugin` runner path. It does not call
`autumn_web::app()` and does not install `HarvestPlugin`. Instead it builds a pool, calls
`HarvestEmbedding::start`, and serves the returned router on a raw Axum server.

`HarvestEmbedding` (issue #1613) runs the startup sequence the plugin runs:

- It applies `[harvest.startup] orphaned_workflows` from `autumn.toml`,
  `autumn-{profile}.toml` or `AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS`.
- `with_ambient_profile()` makes it read the deployment profile from `AUTUMN_ENV` or
  `AUTUMN_PROFILE`. Without it, the profile is `unknown`. The admin API and every mutating
  route then fail closed (issue #1802).
  Declare a credential with `with_admin_auth(StandaloneAdminAuth::new().with_api_tokens())`.
- It loads the persisted admission gates before the worker starts.
- It installs the storage pool and the API runtime, in that order.

`HarvestEmbeddingRuntime::stop` drains the worker and removes the process globals.

The workflow is intentionally smaller than the billing Autumn app, but it still uses the same
reference ideas: a saga reserves inventory with rollback, a child workflow buys the shipping
label, and a version gate selects the v2 shipping payload. The point is runner ownership, not web
framework ceremony.

## Run

```bash
docker compose -f examples/standalone-runner/compose.yaml up -d

DATABASE_URL=postgres://runner:runner@localhost:5434/runner \
AUTUMN_PROFILE=dev \
cargo run -p standalone-runner
```

The raw Axum process listens on `http://localhost:8082`.

- Runner health route: `GET /`
- Harvest API: `GET /api/harvest/health`
- Start workflow: `POST /api/harvest/workflows/standalone_order/start`
- Prometheus scrape endpoint: `GET /metrics` — `HarvestMetricsRecorder::render_prometheus()`
  (issue #1611), the framework-neutral counterpart of the plugin path's
  `/actuator/prometheus`. No `autumn_web::actuator` endpoint is mounted here at all.

```bash
curl -s http://localhost:8082/metrics | grep ^harvest_
```

Run the deployment preflight before starting work:

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:8082/api/harvest preflight
```

```bash
curl -s -X POST http://localhost:8082/api/harvest/workflows/standalone_order/start \
  -H 'Content-Type: application/json' \
  -d '{
    "workflow_id":"order-1001",
    "input":{"order_id":"order-1001","sku":"sku-book","quantity":2}
  }' | jq .
```

Use `examples/billing-autumn-web` when you want the full Autumn web integration with app routes,
outbox publication, saga rollback, child workflow orchestration, version fencing, scheduled DAGs,
signals, timers, and the plugin-managed runner. Use this one when you want to see the runner
wired manually.
