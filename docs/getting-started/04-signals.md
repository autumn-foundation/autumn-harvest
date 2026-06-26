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

## Condition Waiting: `await_condition` and `await_condition_timeout`

Often, a workflow needs to wait until a complex combination of local state changes (e.g., collecting a quorum of approvals) is met. Instead of writing tedious manual loops, you can use the `await_condition` and `await_condition_timeout` primitives.

Below is a comparison of collecting a quorum of 2 approvals manually vs. using `await_condition`.

### Manual Signal-Looping vs. `await_condition`

```rust
// --- Manual Signal-Looping ---
#[workflow]
async fn collect_approvals_manual(ctx: &WorkflowContext) -> HarvestResult<Value> {
    let mut approvals = 0;
    while approvals < 2 {
        let _payload = ctx.wait_for_signal("approved").await?;
        approvals += 1;
    }
    // Perform subsequent action...
    Ok(json!({ "status": "approved" }))
}
```

```rust
// --- Clean Declarative await_condition ---
#[workflow]
async fn collect_approvals_clean(ctx: &WorkflowContext) -> HarvestResult<Value> {
    let mut approvals = 0;

    // Await condition timeout races our condition closure against a timer
    let met_fut = ctx.await_condition_timeout("deadline", 86400, || {
        approvals >= 2
    });
    tokio::pin!(met_fut);

    let mut success = false;
    while approvals < 2 {
        // Check if our condition/timer already resolved early
        if let std::task::Poll::Ready(val) = futures::poll!(&mut met_fut) {
            success = val?;
            break;
        }

        // Wait for the next approved signal, raced against the timeout deadline
        let sig_fut = ctx.wait_for_signal("approved");
        tokio::pin!(sig_fut);

        match futures::future::select(sig_fut, &mut met_fut).await {
            futures::future::Either::Left((sig_res, _)) => {
                if sig_res.is_ok() {
                    approvals += 1;
                }
            }
            futures::future::Either::Right((timeout_res, _)) => {
                success = timeout_res?;
                break;
            }
        }
    }

    // If we completed the loop (approvals >= 2) but didn't resolve met_fut yet,
    // await it now to get the final outcome.
    if approvals >= 2 && !success {
        success = met_fut.await?;
    }

    Ok(json!({ "status": if success { "approved" } else { "timed_out" } }))
}
```

### Determinism Warning
The predicate closure passed to `await_condition` is evaluated multiple times during replay. It **must be deterministic** and rely purely on rehydrated local variables. Never read system time (`Instant::now()`) or generate random values inside the closure, otherwise you will trigger non-determinism replay failures (see rule `HVG008` in the [Workflow Determinism Guide](../workflow-determinism-guide.md)).

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

#### Exactly-once delivery with an idempotency key (issue #521)

Cross-shard delivery is *at-least-once*: the outbox may re-attempt a delivery
after a crash, which can land two `SignalReceived` events on the target. When the
target's handler is not naturally idempotent, supply a delivery key with
`ctx.signal_external_workflow_with_idempotency`:

```rust
ctx.signal_external_workflow_with_idempotency(
    target,
    "onboarding_outcome",
    json!({ "cancelled": true }),
    format!("cancel:{}", target),   // any String or Some(String)
).await?;
```

The key is persisted in the `ExternalSignalRequested` event and deduplicated
against the target's partial unique index, so re-delivery (crash recovery or
outbox retry) lands **exactly one** `SignalReceived` event. The recorded key is
reused verbatim on replay, so a later code change to the key expression cannot
diverge an in-flight delivery. Omitting the key (the plain
`signal_external_workflow` method) keeps the legacy at-least-once behavior. Dedupe
scope is shard-local, keyed on `(target_execution_id, idempotency_key)` — the same
scope as [signal-with-start](../management-api.md).

From a typed client, the `#[signal]` macro generates both a plain
`signal_[name]` stub method and an idempotent `signal_[name]_idempotent` sibling
that takes a trailing `idempotency_key: impl Into<Option<String>>` and returns
`Ok(true)` when freshly queued / `Ok(false)` when the key deduplicated.

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

## Idempotent standalone signals over HTTP (issue #521)

The management route `POST /api/harvest/workflows/{id}/signal/{signal_name}`
delivers a signal to an already-running execution. Webhook providers retry
deliveries, so the same logical event can arrive several times. To collapse
duplicate deliveries into a single `SignalReceived` event, supply an
out-of-band exactly-once key — the request body stays the raw signal payload:

- `Idempotency-Key:` HTTP header, **or**
- `?idempotency_key=` query parameter.

The header wins when both are present. The response reports whether the signal
was freshly queued:

```bash
# First delivery — queued.
curl -X POST '/api/harvest/workflows/<exec-id>/signal/approval' \
  -H 'Idempotency-Key: evt_abc123' \
  -H 'Content-Type: application/json' \
  -d '{"approved": true}'
# 202 { "ok": true, "signal_delivered": true }

# Retry with the same key — deduplicated, no second handler run.
curl -X POST '/api/harvest/workflows/<exec-id>/signal/approval' \
  -H 'Idempotency-Key: evt_abc123' \
  -H 'Content-Type: application/json' \
  -d '{"approved": true}'
# 202 { "ok": true, "signal_delivered": false }
```

Dedupe scope is shard-local, keyed on `(execution_id, idempotency_key)`
(matching signal-with-start, #244). Omitting the key reproduces the legacy
at-least-once behavior exactly — every call delivers a distinct signal event.

### The saga-choreography example

`examples/saga-choreography/` shows the complete "tenant cancel notifies all
in-flight per-tenant onboarding workflows" pattern. Run its replay tests with:

```bash
cargo test -p saga-choreography
```

---

[← Durable timers](03-durable-timers.md) · [Index](README.md) · [Next: Child workflows →](05-child-workflows.md)
