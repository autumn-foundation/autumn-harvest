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

---

[← Child workflows](05-child-workflows.md) · [Index](README.md) · [Next: Reliability knobs →](07-reliability-knobs.md)
