# Chapter 6 — Idempotency for safe retries

[← Child workflows](05-child-workflows.md) · [Index](README.md) · [Next: Reliability knobs →](07-reliability-knobs.md)

---

Activities are at-least-once. If the worker crashes after Stripe accepts a
charge but before the engine writes `ActivityCompleted` to Postgres, the
retry will charge the customer again unless the downstream system
deduplicates. Every activity gets a stable, retry-safe key from
`ctx.idempotency_key()`:

```rust
#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(2)))]
async fn charge_card(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let amount_cents = input["amount_cents"].as_u64().unwrap_or(0);
    let customer_id = input["customer_id"].as_str().unwrap_or("").to_owned();

    let idem_key = ctx.idempotency_key()?.as_str().to_owned();

    // Pass idem_key as Stripe's Idempotency-Key header. Subsequent retries
    // for this attempt carry the same key, so Stripe returns the original
    // charge instead of creating a new one.
    let charge_id = stripe_charge(amount_cents, &customer_id, &idem_key)
        .await
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "charge_id": charge_id }))
}
```

The key is stable across worker restarts, duplicate dispatch, and replay,
**but it's distinct for every logical invocation** — calling `charge_card`
twice for two different orders gets two different keys.

For activities that make several outbound calls, derive named subkeys:

```rust
let key = ctx.idempotency_key()?;
create_db_user(&user_id, key.subkey("db").as_str()).await?;
send_welcome_email(&user_id, key.subkey("email").as_str()).await?;
```

## Idempotent signal delivery

Idempotency also matters in the other direction: webhook and event sources
(Stripe, GitHub, SQS) deliver **at-least-once**, so the same logical event can
reach a running workflow twice. Supplying an idempotency key with the signal
collapses duplicate deliveries into exactly one `SignalReceived` event — no
hand-rolled "seen event ids" dedup set in the workflow body (issues #521/#753):

- **HTTP** — `POST /workflows/{id}/signal/{signal_name}` with an
  `Idempotency-Key:` header (or `?idempotency_key=` query param; the header
  wins when both are present).
- **CLI** — `harvest workflow signal <exec-id> <name> --idempotency-key <key>`.
- **Typed client stub** — the `#[signal]` macro generates a
  `signal_{name}_idempotent(...)` sibling method.
- **Untyped client** — `signal::send_signal_idempotent(conn, exec_id, name,
  payload, Some(key))`.
- **First delivery for a workflow** — `signal-with-start` carries its own
  `idempotency_key` field (issue #244); the surfaces above cover the
  steady-state "signal an already-running workflow" case.

Dedupe scope is per execution — `(execution_id, idempotency_key)` — so the
same upstream event id may safely target different executions. A deduplicated
delivery returns 2xx with `signal_delivered: false` — deliberately even when
the execution has since gone terminal, as long as the key originally landed
while the run was still active (a retry acknowledges a delivery that already
happened; a fresh key or an unkeyed signal to a terminal run still gets the
terminal error). Omitting the key preserves the legacy at-least-once behavior
exactly. Full contract and curl examples:
[signals chapter](04-signals.md#idempotent-standalone-signals-over-http-issue-521)
and the [signal-delivery section of the management-API reference](../management-api.md#signal-delivery-post-workflowsidsignalsignal_name).

---

[← Child workflows](05-child-workflows.md) · [Index](README.md) · [Next: Reliability knobs →](07-reliability-knobs.md)
