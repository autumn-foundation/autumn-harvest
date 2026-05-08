# Chapter 4 — Signals: waiting on the outside world

[← Durable timers](03-durable-timers.md) · [Index](README.md) · [Next: Child workflows →](05-child-workflows.md)

---

Real workflows wait on humans, webhooks, or other systems. Signals are
named, payload-carrying messages delivered over the management API and
buffered durably until the workflow consumes them.

Add a payment-confirmation hand-off:

```rust
#[workflow]
async fn checkout(ctx: &WorkflowContext, order_id: String) -> HarvestResult<String> {
    ctx.execute_activity_raw(
        "reserve_inventory",
        serde_json::json!({ "order_id": order_id }),
        "default",
    )
    .await?;

    // Block until the payment gateway calls back.
    let payload = ctx.wait_for_signal("payment_captured").await?;
    let capture_id = payload["capture_id"].as_str().unwrap_or("").to_owned();

    ctx.execute_activity_raw(
        "fulfill_order",
        serde_json::json!({ "order_id": order_id, "capture_id": capture_id }),
        "default",
    )
    .await?;

    Ok(capture_id)
}
```

Start the workflow:

```bash
curl -s -X POST http://localhost:3000/api/harvest/workflows/checkout/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"order-42","input":"order-42"}' | jq .
```

It will run `reserve_inventory`, then suspend on the signal. Find the
execution ID in the response (or `harvest workflow list`), then deliver the
signal:

```bash
curl -s -X POST \
  http://localhost:3000/api/harvest/workflows/<EXECUTION_ID>/signal/payment_captured \
  -H 'Content-Type: application/json' \
  -d '{"capture_id":"cap_demo_123"}' | jq .
```

The workflow wakes up, runs `fulfill_order`, and completes.

> Signals delivered while the workflow isn't currently waiting are buffered.
> A workflow that hasn't reached its `wait_for_signal` call yet will see the
> already-arrived payload as soon as it gets there.

---

[← Durable timers](03-durable-timers.md) · [Index](README.md) · [Next: Child workflows →](05-child-workflows.md)
