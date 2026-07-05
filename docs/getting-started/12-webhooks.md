# Chapter 12: Inbound webhooks

Webhook ingestion (Stripe events, GitHub PRs, Twilio messages, vendor
callbacks) is the most common production trigger for the workflows Harvest
runs. This chapter wires a Stripe webhook straight into a durable workflow
start, in under 50 lines of your own code.

## Who verifies what

Harvest does **not** ship its own signature verification, timestamp
tolerance, or replay protection. Autumn's `[security.webhooks]` layer
(`autumn_web::webhook::SignedWebhook`) already does that — for Stripe,
GitHub, Slack, and generic HMAC providers — with secret rotation and a
boot-time check that a production deployment can't start with a missing or
weak secret. Harvest's `#[webhook]` macro sits entirely downstream: it maps
an **already-verified** delivery to a deterministic workflow trigger and
dispatches it idempotently.

## 1. Configure the endpoint

```toml
# autumn.toml
[[security.webhooks.endpoints]]
name = "stripe"
path = "/hooks/stripe"
provider = "stripe"
secret_env = "STRIPE_WEBHOOK_SECRET"
# Harvest's own dedup (the mapping function's deterministic WorkflowId, or
# the SignalsWithStart idempotency key) is durable and survives restarts —
# autumn-web's replay-protection layer is in-memory by default. Recommended
# for Harvest-bound endpoints so a redelivery resolves to the same execution
# (200 + the original workflow_exec_id) instead of a 409.
replay_protection = false
```

## 2. Write the mapping function

```rust
use autumn_harvest::prelude::*;

#[derive(serde::Deserialize)]
struct StripeEvent {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
}

#[webhook(path = "/hooks/stripe", starts = "subscription_flow")]
fn map_stripe_event(_ctx: &WebhookCtx, evt: StripeEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("stripe-{}", evt.id)))
}
```

`ctx: &WebhookCtx` carries the already-verified metadata (`provider()`,
`delivery_id()`, `event_type()`, the endpoint name); the second parameter is
your typed payload, deserialized from the verified body. The function is
**synchronous** — do I/O inside the target workflow, not here. Return the
deterministic `WorkflowId` Harvest should start (or attach to, if it's
already running).

## 3. Wire the plugin

```rust
let app = autumn_web::app()
    .plugin(
        HarvestPlugin::new()
            .workflows(workflows![subscription_flow])
            .webhooks(webhooks![map_stripe_event])
            .worker(WorkerConfig::default())
            .api("/api/harvest"),
    );
```

`HarvestPlugin::build` mounts `/hooks/stripe` as an app-level route (not
behind the `HarvestPlugin::api_with_auth` management-API auth layer — the
HMAC signature *is* the auth for this route) and fails fast at startup if
two webhooks declare the same path, or if a trigger targets a workflow you
forgot to register.

## 4. Try it

```bash
BODY='{"id":"evt_123","type":"invoice.payment_succeeded"}'
TS=$(date +%s)
SECRET=$STRIPE_WEBHOOK_SECRET
SIG=$(printf "%s.%s" "$TS" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.* //')

curl -X POST http://localhost:8080/hooks/stripe \
  -H "Stripe-Signature: t=$TS,v1=$SIG" \
  -H "Content-Type: application/json" \
  -d "$BODY"
# {"status":"accepted","workflow_exec_id":"...","workflow_id":"stripe-evt_123"}

# Redeliver the identical event — same exec id, no duplicate execution:
curl -X POST http://localhost:8080/hooks/stripe \
  -H "Stripe-Signature: t=$TS,v1=$SIG" \
  -H "Content-Type: application/json" \
  -d "$BODY"
# {"status":"idempotent_replay","workflow_exec_id":"...","workflow_id":"stripe-evt_123"}
```

## Signaling a running workflow instead of starting one

Stripe's `invoice.payment_succeeded` often needs to reach an *already
running* subscription workflow rather than start a new one. Use `signals`
instead of `starts` — it atomically start-or-attaches and delivers a signal
(the `signal_with_start` primitive from Chapter 4), keyed by the verified
delivery ID:

```rust
#[webhook(
    path = "/hooks/stripe/payments",
    signals = "subscription_flow",
    signal_name = "payment_succeeded",
)]
fn map_payment(_ctx: &WebhookCtx, evt: StripeEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new("subscription-shared-id"))
}
```

A `signals` target **requires** the endpoint to resolve a delivery ID
(configured `delivery_id_header`, or a top-level `"id"` JSON field — Stripe's
default). That ID feeds the signal's idempotency key, but the raw ID is not
used verbatim: it's namespaced with the webhook's `path` and `signal_name`
(`{path}:{signal_name}:{delivery_id}`) before being handed to
`signal_with_start`. `signal_with_start`'s own dedupe is scoped to
`(workflow_name, workflow_id, idempotency_key)` only — it has no notion of
webhook endpoints — so without namespacing, two different `#[webhook(signals
= ...)]` bindings that both target the same `(workflow_name, workflow_id)`
pair (an entity workflow fed by more than one provider) could collide
whenever their raw delivery IDs happened to match, silently dropping the
second signal as an "idempotent replay" of the first. A `starts` target
needs no delivery ID; its own returned `WorkflowId` is the idempotency
mechanism.

## Response shapes

| Outcome | Status | Body |
|---|---|---|
| Fresh dispatch | `202 Accepted` | `{"status":"accepted","workflow_exec_id","workflow_id"}` |
| Idempotent redelivery | `200 OK` | `{"status":"idempotent_replay","workflow_exec_id","workflow_id"}` |
| Verification failed | `401`/`400`/`409`/`503` | autumn-web's own structured error (signature/timestamp/replay) |
| Body not valid JSON | `400` | `{"error_code":"parse_failed","error"}` |
| Mapping function rejected the payload | `400` | `{"error_code":"mapping_rejected","error"}` |
| `signals` target, no delivery ID resolved | `400` | `{"error_code":"missing_idempotency","error"}` |

## Metrics and audit

`harvest.webhook.received` / `harvest.webhook.rejected` counters (labels
`path`, `outcome`; `path` is bounded to your registered `#[webhook]`
bindings) — see `docs/telemetry.md`. Every dispatch *attempt* that passes
verification writes an audit row (`operation = "webhook.trigger"`); a failed
verification never does (an unauthenticated sender can't manufacture audit
writes).

## What's out of scope

Vendor-specific verifiers ship with autumn-web (`WebhookProvider::Stripe`
/`Github`/`Slack`/`Generic`), not Harvest — there is no `WebhookVerifier`
trait here. Outbound webhooks (a workflow calling out to an external system)
are a plain `#[activity]`; Harvest also ships a durable outbound-delivery
slice behind the same `webhooks` cargo feature (`docs/telemetry.md` and
`examples/` reference it) if you want retries/DLQ for outbound calls too.
