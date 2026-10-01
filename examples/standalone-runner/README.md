# standalone-runner

This example embeds Harvest in a plain Axum server. Its `Cargo.toml` has no
`autumn-web` entry (issue #1615). It does not call `autumn_web::app()` and does
not install `HarvestPlugin`.

`autumn-harvest-plugin` still depends on `autumn-web`, so the crate is in the
build graph. The point is that the embedder never names it. A change that puts
an `autumn_web::` type back into the standalone surface fails to compile here.
Two tests in `src/tests.rs` also fail when the manifest or the source names
`autumn-web`.

| Need | How the example does it, with no `autumn-web` |
|---|---|
| Storage pool | `diesel_async` `deadpool` pool (`src/db.rs`) |
| Migrations | `autumn_harvest::migrate`, the code behind `harvest migrate run` (`src/db.rs`) |
| Management API and Vantage | `HarvestEmbedding::start`, nested on an `axum::Router` (`src/server.rs`) |
| Operator credential | `StandaloneAdminAuth::with_api_tokens()` and `harvest token bootstrap` |
| Metrics | `GET /metrics` from `HarvestMetricsRecorder::render_prometheus()` |
| Webhooks | `build_webhook_router` with a `#[webhook]` binding (`src/webhooks.rs`) |

`HarvestEmbedding` (issue #1613) runs the startup sequence the plugin runs. It
applies `[harvest.startup]`, reads the profile from `AUTUMN_ENV` or
`AUTUMN_PROFILE`, loads the admission gates, and installs the pool and the API
runtime. `HarvestEmbeddingRuntime::stop` drains the worker on shutdown.

The workflow is small, but it uses the reference ideas of the billing app. A
saga reserves inventory with rollback, a child workflow buys the shipping
label, and a version gate selects the v2 shipping payload.

## Run

```bash
docker compose -f examples/standalone-runner/compose.yaml up -d

DATABASE_URL=postgres://runner:runner@localhost:5434/runner \
AUTUMN_PROFILE=dev \
cargo run -p standalone-runner
```

In the `dev` profile the runner applies the Harvest migrations itself. The
`dev` admin API needs no credential. Do not expose a `dev` process beyond
localhost.

| Variable | Default | Use |
|---|---|---|
| `DATABASE_URL` | `postgres://runner:runner@localhost:5434/runner` | The Harvest database |
| `AUTUMN_PROFILE` | none (`unknown`) | Deployment profile. `dev` also applies migrations. |
| `STANDALONE_RUNNER_ADDR` | `127.0.0.1:8082` | Listen address |
| `STANDALONE_RUNNER_WEBHOOK_SECRET` | none | HMAC secret. The webhook route exists only when this is set. |

Routes:

- `GET /`: runner health.
- `GET /api/harvest/health`: Harvest health.
- `GET /api/harvest/ui`: the Vantage dashboard.
- `POST /api/harvest/workflows/standalone_order/start`: start an order.
- `GET /metrics`: Prometheus text.
- `POST /hooks/orders`: the signed order webhook.

```bash
curl -s -X POST http://localhost:8082/api/harvest/workflows/standalone_order/start \
  -H 'Content-Type: application/json' \
  -d '{
    "workflow_id":"order-1001",
    "input":{"order_id":"order-1001","sku":"sku-book","quantity":2}
  }' | jq .

curl -s http://localhost:8082/metrics | grep ^harvest_
```

Run the deployment preflight:

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:8082/api/harvest preflight
```

## Run outside `dev`

Outside `dev`, the runner does not migrate, and every admin route needs a
credential. Apply the migrations and seed the first API token first:

```bash
export DATABASE_URL=postgres://runner:runner@localhost:5434/runner
HARVEST_DATABASE_URL="$DATABASE_URL" cargo run -p autumn-harvest-cli -- migrate run
cargo run -p autumn-harvest-cli -- token bootstrap
```

`token bootstrap` prints a secret and an `INSERT` statement. Run the statement
against the database, and keep the secret. Then start the runner and send the
secret as a bearer token:

```bash
AUTUMN_PROFILE=prod cargo run -p standalone-runner

HARVEST_TOKEN=<secret> cargo run -p autumn-harvest-cli -- \
  --base-url http://localhost:8082/api/harvest preflight
```

The token gates the admin routes only. The other routes, for example a
workflow start, have no auth here. Put your own auth layer in front of
`harvest.router()` in production. Until then, `preflight` reports
`admin_auth_boundary` as `fail` outside `dev`. That result is correct.

## Webhooks

```bash
STANDALONE_RUNNER_WEBHOOK_SECRET=<at least 16 bytes> \
AUTUMN_PROFILE=dev cargo run -p standalone-runner
```

Sign the raw body with HMAC-SHA256 and send it as
`X-Webhook-Signature: sha256=<hex>`. The `order_placed` binding starts
`standalone_order` with the workflow id `order-<order_id>`.

## Test

```bash
cargo test -p standalone-runner --bins
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p standalone-runner --test acceptance -- --test-threads=1
```

The `acceptance` suite runs the built binary against Postgres and drives the
steps above over HTTP. Without `HARVEST_TEST_DATABASE_URL`, it starts a
Postgres container.

Use `examples/billing-autumn-web` for the full Autumn web integration through
`HarvestPlugin`. Use this one to embed the runner in any Axum service.
