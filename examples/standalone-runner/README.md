# standalone-runner

This example embeds Harvest in a plain Axum server. Its `Cargo.toml` has no
`autumn-web` entry (issue #1615). It does not call `autumn_web::app()` and does
not install `HarvestPlugin`.

`autumn-harvest-plugin` still depends on `autumn-web`, so the crate is in the
build graph. The point is that the embedder never names it. A plugin API that
needs an `autumn_web::` type with no plugin re-export fails to compile here.
Two tests in `src/tests.rs` also fail when the manifest or the source names
`autumn-web`.

Some types still come from `autumn-web`. The webhook config types are
`autumn-web` types that `autumn_harvest_plugin::webhook_receiver` re-exports.
`HarvestEmbedding::start` returns an `autumn-web` error, which the example
only formats.

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
localhost. Ctrl-C or SIGTERM drains the worker and stops the process. Open
responses, such as an SSE stream, get 10 seconds before the server closes
them.

| Variable | Default | Use |
|---|---|---|
| `DATABASE_URL` | `postgres://runner:runner@localhost:5434/runner` | The Harvest database. See [TLS](#tls). |
| `AUTUMN_ENV` or `AUTUMN_PROFILE` | none (`unknown`) | Deployment profile. `AUTUMN_ENV` wins. `dev` or `development` also applies migrations. |
| `STANDALONE_RUNNER_ADDR` | `127.0.0.1:8082` | Listen address. Port `0` picks a free port. Some routes have no auth, so keep it on localhost or behind your own auth layer. |
| `STANDALONE_RUNNER_WEBHOOK_SECRET` | none | HMAC secret. The webhook route exists only when this is set. Outside `dev`, a secret under 32 bytes refuses boot. |

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

Outside `dev`, the runner does not migrate. Every admin route needs a
credential. Apply the migrations and seed the first API token first:

```bash
export DATABASE_URL=postgres://runner:runner@localhost:5434/runner
HARVEST_DATABASE_URL="$DATABASE_URL" cargo run -p autumn-harvest-cli -- migrate run
cargo run -p autumn-harvest-cli -- token bootstrap
```

`token bootstrap` prints a secret and an `INSERT` statement. Run the statement
against the database, and keep the secret. Then start the runner and send the
secret as a bearer token. `read -rs` keeps the secret out of the shell history:

```bash
AUTUMN_PROFILE=prod cargo run -p standalone-runner

read -rs HARVEST_TOKEN && export HARVEST_TOKEN
cargo run -p autumn-harvest-cli -- --base-url http://localhost:8082/api/harvest preflight
```

The token gates the admin routes only. A workflow start and the workflow reads
have no auth here. For production, put your own auth layer in front of
`harvest.router()`. Then declare it with
`StandaloneAdminAuth::with_admin_auth_boundary()`.

Until you do, `preflight` reports `admin_auth_boundary` as `fail` under a
named non-`dev` profile. It reports `warn` when the profile is unknown. Both
results are correct. The acceptance suite pins the `fail`.

## TLS

`sslmode=require`, `verify-ca` and `verify-full` connect through rustls. The
server certificate must chain to the platform trust store in all three modes.
`verify-full` and `require` also check the host name, so `require` is
stricter here than in libpq. `verify-ca` checks the chain only. The runner
reads `sslmode` with the libpq grammar. `sslrootcert` is not read. Put a
private CA in the platform store, or point `SSL_CERT_FILE` at a bundle that
holds it. A URL with no `sslmode`, `disable` or `prefer` connects in
plaintext.

## Webhooks

```bash
STANDALONE_RUNNER_WEBHOOK_SECRET=<random secret, 32 bytes or more> \
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
