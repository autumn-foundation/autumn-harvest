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

## Signaling another workflow

You can push a typed signal to any other running workflow directly from inside
a workflow function — no activity, no HTTP call, no hand-rolled outbox required.

```rust
#[workflow]
async fn tenant_cancel(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    let onboarding_ids: Vec<ExecutionId> = /* load from input */;

    for target in onboarding_ids {
        match ctx
            .signal_external_workflow(target, "onboarding_outcome", json!({"cancelled": true}))
            .await
        {
            Ok(()) => { /* signal durably accepted for delivery */ }
            Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => {
                // Workflow already finished — safe to skip in a fan-out cancel.
                tracing::info!(%target, %reason_code, "onboarding already done");
            }
            Err(e) => return Err(e),
        }
    }
    Ok(json!({ "cancelled": onboarding_ids.len() }))
}
```

`ctx.signal_external_workflow(target, signal_name, payload)` is deterministic and
replay-safe: on the first live call it appends an `ExternalSignalRequested` event
and attempts delivery; the terminal outcome (`ExternalSignalDelivered` or
`ExternalSignalFailed`) is also recorded. On replay the recorded outcome is returned
immediately without re-issuing any side effect.

### Reason codes

| `reason_code` | Meaning |
|---|---|
| `"target_terminal"` | The target workflow is already in a terminal state (completed, failed, cancelled). |
| `"target_unknown"` | The target `ExecutionId` was not found. Usually a typo or a race where the target workflow has not yet been persisted. |
| `"cross_shard_unsupported"` | The target lives on a different database shard. Cross-shard delivery requires the plugin's outbox extension (see below). |

### Cross-shard delivery guarantee

For same-shard targets, delivery is transactional: the signal row is written
atomically with the history event and the target's task is woken via
LISTEN/NOTIFY. For cross-shard targets (when `target.shard()` differs from the
caller's shard) the signal is forwarded through the plugin's outbox worker
(`autumn-harvest-plugin`), which delivers it asynchronously without a
cross-shard transaction. The workflow observes `Ok(())` once the outbox write is
durable — the signal is guaranteed to reach the target eventually or the outbox
will surface a permanent failure reason.

### The saga-choreography example

`examples/saga-choreography/` shows the complete "tenant cancel notifies all
in-flight per-tenant onboarding workflows" pattern. Run its replay tests with:

```bash
cargo test -p saga-choreography
```

---

[← Durable timers](03-durable-timers.md) · [Index](README.md) · [Next: Child workflows →](05-child-workflows.md)
