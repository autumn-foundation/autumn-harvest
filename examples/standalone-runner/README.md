# standalone-runner

This example shows the out-of-the-box non-`HarvestPlugin` runner path. It does not call
`autumn_web::app()` and does not install `HarvestPlugin`. Instead it builds Harvest directly,
starts `HarvestRunner`, installs the runner's API runtime into `HarvestApiState`, and mounts the
management router on a raw Axum server.

The workflow is intentionally smaller than the billing Autumn app, but it still uses the same
reference ideas: a saga reserves inventory with rollback, a child workflow buys the shipping
label, and a version gate selects the v2 shipping payload. The point is runner ownership, not web
framework ceremony.

## Run

```bash
docker compose -f examples/standalone-runner/compose.yaml up -d

$env:DATABASE_URL = "postgres://runner:runner@localhost:5434/runner"
$env:AUTUMN_PROFILE = "dev"
cargo run -p standalone-runner
```

The raw Axum process listens on `http://localhost:8082`.

- Runner health route: `GET /`
- Harvest API: `GET /api/harvest/health`
- Start workflow: `POST /api/harvest/workflows/standalone_order/start`

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
