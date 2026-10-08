# billing-autumn-web

A full Autumn web integration for autumn-harvest. This is the reference example for the
less tiny path: app routes, outbox workflow publication, `HarvestPlugin`, management API,
worker, scheduler, saga compensation, child workflow composition, version gates, signals,
timers, deterministic side effects, and a scheduled DAG.

It models a subscription checkout. The web route accepts a billing request, writes a
workflow-start row into the application outbox, and the Harvest plugin relays it into the
durable workflow store. The workflow uses a saga for customer/payment/subscription rollback,
spawns a child workflow to issue the invoice, fences the v2 tax path with `ctx.version`, waits
for a `payment_captured` signal, and records payment capture before sending a receipt. The
`monthly_billing_cycle` workflow shows continue-as-new for long-running billing periods.

## Run

```bash
docker compose -f examples/billing-autumn-web/compose.yaml up -d

AUTUMN_MANIFEST_DIR=examples/billing-autumn-web \
AUTUMN_PROFILE=dev \
cargo run -p billing-autumn-web
```

The app listens on `http://localhost:8081`.

- App route: `POST /billing/checkout`
- Plans route: `GET /billing/plans`
- Harvest API: `GET /api/harvest/health`
- Harvest UI: `GET /api/harvest/ui`

Run the deployment preflight before starting work:

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:8081/api/harvest preflight
```

## Start Checkout Through The App

```bash
curl -s -X POST http://localhost:8081/billing/checkout \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id":"acme",
    "customer_id":"cust_42",
    "plan":"pro",
    "seats":5,
    "payment_method_id":"pm_card_demo"
  }' | jq .
```

The outbox relay starts `billing_checkout` asynchronously. Find the execution in the UI or:

```bash
curl -s 'http://localhost:8081/api/harvest/workflows?workflow_name=billing_checkout&search_attr=tenant_id=acme' | jq .
```

Then deliver the gateway callback signal:

```bash
curl -s -X POST http://localhost:8081/api/harvest/workflows/<EXECUTION_ID>/signal/payment_captured \
  -H 'Content-Type: application/json' \
  -d '{"captured":true,"capture_id":"cap_demo_123"}' | jq .
```

## Useful Direct API Calls

Start the same workflow without the application outbox:

```bash
curl -s -X POST http://localhost:8081/api/harvest/workflows/billing_checkout/start \
  -H 'Content-Type: application/json' \
  -d '{
    "workflow_id":"checkout-direct-1",
    "input":{
      "tenant_id":"acme",
      "customer_id":"cust_42",
      "plan":"pro",
      "seats":5,
      "payment_method_id":"pm_card_demo"
    },
    "search_attrs":{"tenant_id":"acme","customer_id":"cust_42","plan":"pro"}
  }' | jq .
```

Run the reconciliation DAG:

```bash
curl -s -X POST http://localhost:8081/api/harvest/dags/billing_reconciliation/trigger \
  -H 'Content-Type: application/json' \
  -d '{"conf":{"date":"2026-05-01"}}' | jq .
```

The standalone runner example in `examples/standalone-runner` uses the same saga, child workflow,
version, and runner vocabulary from the other side of the integration: no `HarvestPlugin`, just
`HarvestRunner` and a manually mounted management router.
