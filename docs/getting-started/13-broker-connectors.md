# Chapter 13: Broker connectors (Kafka, SQS)

A message on a Kafka topic or an SQS queue is the other most common production
trigger for a durable workflow, alongside an inbound webhook
([chapter 12](12-webhooks.md)). This chapter binds one to a workflow — with
idempotent redelivery, correct ack ordering, poison isolation and backpressure
— in under 20 lines of your own code.

## The core engine never sees your broker

The connector lives in `autumn-harvest-plugin`, behind Cargo features. The
`autumn-harvest` engine crate stays Postgres-only:

```bash
cargo tree -p autumn-harvest --all-features | grep -E 'rdkafka|aws-sdk-sqs'   # empty
```

That is not a convention — `autumn-harvest-plugin/tests/connector_dependency_graph.rs`
runs exactly that query in CI and fails the build if a broker client ever
reaches the engine's graph. Everything the engine contributes is
dependency-free: four `harvest.connector.*` metric constants with no-op
`MetricsRecorder` defaults, and the additive `StartSource::Broker` provenance
value.

Features:

| Feature | Brings |
|---|---|
| `connectors` | The broker-**agnostic** layer: bindings, idempotency, ack ordering, poison isolation, backpressure, and `MockSource`. No broker client at all — enough to unit-test your mapping function and the whole dispatch path with no Docker. |
| `kafka` | `connectors` + `rdkafka`. |
| `sqs` | `connectors` + `aws-config` / `aws-sdk-sqs`. |

Building with `kafka` compiles vendored librdkafka, which needs libcurl headers
(`libcurl4-openssl-dev` on Debian/Ubuntu).

## 1. Kafka → a workflow start

```rust
use std::sync::Arc;
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::connector::{
    KafkaSource, KafkaSourceConfig, MappedMessage, SourceBinding,
};

#[derive(serde::Deserialize, serde::Serialize)]
struct OrderPlaced { order_id: String, total_cents: i64 }

#[workflow]
async fn fulfil_order(_ctx: &WorkflowContext, order: OrderPlaced) -> Result<String, String> {
    Ok(format!("fulfilled {}", order.order_id))
}

#[autumn_web::main]
async fn main() {
    let source = Arc::new(
        KafkaSource::connect(&KafkaSourceConfig::new(
            "localhost:9092", "harvest-orders", "orders.placed",
        ))
        .expect("kafka consumer should connect"),
    );

    let binding = SourceBinding::starts("orders", "orders.placed", "fulfil_order")
        .map_json(|_ctx, order: OrderPlaced| {
            let payload = serde_json::to_value(&order).map_err(|e| e.to_string())?;
            Ok::<_, String>(MappedMessage::new(format!("order-{}", order.order_id), payload))
        })
        .max_in_flight(32);

    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![fulfil_order])
                .connector(binding, source)
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
```

Runnable: [`examples/kafka_connector_quickstart.rs`](../../autumn-harvest-plugin/examples/kafka_connector_quickstart.rs).

`SourceBinding::starts(binding_name, stream, workflow)` says *what* a message
maps to; `KafkaSource` is the adapter that feeds it. The mapping function is
**synchronous** and returns a `MappedMessage`: the workflow id, and the JSON
payload to hand the workflow as its start input. `HarvestPlugin::build` fails
fast (panics) if the target workflow is not registered, the binding has no
mapping function, two bindings share a name, or the adapter's stream does not
match the binding's — misconfigurations that would otherwise surface as a
silently idle consumer.

## 2. SQS → an entity workflow (start-or-signal)

Whenever messages carry an entity key, prefer a `signals_with_start` binding.
The first message for a key starts the run; every later message is delivered to
the *same* run as a signal ([issue #244's atomic start-or-attach](06-idempotency.md)):

```rust
let binding = SourceBinding::signals_with_start(
        "telemetry", "device-telemetry", "device_session", "reading",
    )
    .map_json(|_ctx, r: Reading| {
        let payload = serde_json::to_value(&r).map_err(|e| e.to_string())?;
        Ok::<_, String>(MappedMessage::new(format!("device-{}", r.device_id), payload))
    })
    .broker_native_dead_letter()   // let SQS redrive own poison messages
    .max_in_flight(16);

let source = Arc::new(
    SqsSource::connect(
        SqsSourceConfig::new(QUEUE_URL)
            .stream("device-telemetry")
            .visibility_timeout_secs(60),
    )
    .await
    .expect("sqs client should build"),
);
```

Runnable: [`examples/sqs_connector_quickstart.rs`](../../autumn-harvest-plugin/examples/sqs_connector_quickstart.rs).

Size `visibility_timeout_secs` above your worst-case dispatch latency, or SQS
will redeliver a message that is still in flight. (Redelivery is *safe* — see
idempotency below — but it wastes work.)

## The ack-ordering contract

**A message is acknowledged only after harvest durably owns it.** Concretely:

1. The mapping function runs.
2. The dispatch commits — a `WorkflowStarted` event and execution row, or a
   staged signal — or harvest recognises the message as an already-committed
   replay, or the start throttle defers it (`202`).
3. *Only then* is the message acked: a Kafka offset commit, an SQS
   `DeleteMessage`.

Anything else leaves the message unacked, so the broker redelivers it. Kill the
process between step 2 and step 3 and you get a redelivery, not a lost message —
and because the dedupe key is derived from the message's own broker coordinates,
that redelivery resolves to the *same* execution rather than a second one.
That is the at-least-once half of the contract; step 2's idempotency is what
makes it safe.

For Kafka there is one extra rule. A Kafka offset commit is a **high-water
mark**: committing offset `N` asserts everything below `N` is done. Because the
runtime dispatches concurrently, offsets can finish out of order. The connector
therefore commits only the **contiguous completed prefix** — if offsets 11 and
12 finish while 10 is still in flight, nothing is committed until 10 lands, at
which point all three commit at once. A crash in that window redelivers 10, 11
and 12; all three dedupe.

A redelivery of an offset *at or below* the current mark is already durably
settled (a rebalance replays it), so it is re-acked — a commit is idempotent —
rather than silently withheld.

## Idempotency

The dedupe key is derived from stable broker coordinates, namespaced by the
binding, and passed to harvest's own `idempotency_key` machinery
([chapter 6](06-idempotency.md)):

| Broker | Coordinate |
|---|---|
| Kafka | `topic:partition:offset` |
| SQS FIFO | `MessageDeduplicationId` — producer-controlled, so it survives a *re-publish* of the same logical event too |
| SQS standard | `MessageId` — stable across redeliveries of the same message, but **not** across a re-publish. The honest limit of a standard queue. |

Namespacing by binding means two bindings consuming the same topic never alias
each other. The key is bounded and injectively encoded, so a pathological
coordinate cannot collide with another message's key or blow the column limit.

There is one interaction worth knowing. Harvest's start route rejects an
`idempotency_key` combined with a throttle / debounce / batch admission policy
(they defer the start, so there is no execution id to return). When your target
workflow has one of those policies, the connector automatically falls back to
**workflow-id** dedupe: the mapping function's `workflow_id` is the dedupe unit,
and a redelivery resolves to the same run through the normal id-reuse policy.
Override with `.idempotency_mode(...)` if you want to force one or the other —
forcing `BrokerCoordinates` onto a deferred-admission workflow is rejected at
build time rather than at the first message.

`signals_with_start` bindings always use broker coordinates: the signal path's
key is a body field with no such mutual exclusion.

## Poison messages

A message that can never succeed must not wedge its partition. Two classes:

* **Deterministic** — the body does not decode, or the start route rejects it
  (`4xx`: schema validation, an unregistered workflow). Dead-lettered
  immediately; retrying is pointless.
* **Strike-counted** — the mapping function *rejects* the message (it returned
  `Err`). Retried until `poison_threshold` consecutive rejections (default `3`,
  mirroring `poison_pill_threshold` from issue #367), then dead-lettered. A
  transient rejection — a lookup table that has not loaded yet — gets a few
  chances; a permanently unmappable message does not spin forever.

Setting `poison_threshold(0)` disables the strike counter (retry a rejection
forever) but **not** deterministic dead-lettering, because retrying an
undecodable body forever is exactly the wedge this exists to prevent.

Where it goes depends on the binding:

* Default (`DeadLetterMode::HarvestSink`) — a row in
  `harvest_connector_dead_letters` (a **plugin**-owned table) carrying the
  binding, stream, rendered coordinates, dedupe key, reason, detail, attempt
  count and the **raw payload**, so an operator can replay it by hand after a
  fix. The message is then acked. The `idempotency_key` column is `UNIQUE`, so
  dead-lettering is itself idempotent.
* `.broker_native_dead_letter()` — hand the message back to the broker's own
  machinery instead. For SQS the visibility timeout is reset to `0`, so SQS
  re-delivers, counts the receive, and moves the message to the queue's
  configured DLQ once `maxReceiveCount` is hit.

A transient harvest failure (a `5xx`, a pool exhaustion) is **never**
dead-lettered no matter how many times it recurs — it is not the message's
fault. It stays unacked and is redelivered.

## Backpressure

`max_in_flight` (default `16`) bounds concurrently-dispatched messages per
binding. It composes with the start throttle
([issue #607](07-reliability-knobs.md)): a throttled start returns `202` with no
execution id, and the connector treats that as a **successful** dispatch and
acks. Busy-retrying a deferred start would defeat the throttle and stampede the
admission path; the throttle already owns the pacing and will fire the start
when a token frees up.

## Ordering caveat

**The connector does not preserve broker partition ordering.** It dispatches up
to `max_in_flight` messages concurrently, so two messages from the same
partition can commit out of order. This is deliberate — serialising every
message would cap throughput at one dispatch per round trip.

If you need per-key ordering, do not lower `max_in_flight` to 1 and hope; use
the entity pattern. A `signals_with_start` binding whose mapping function
derives a stable `workflow_id` from the partition key routes every message for
that key into the **same execution**, and a workflow processes its own signals
in the order they were recorded. Ordering then holds per entity — which is
almost always the property you actually wanted — while distinct entities still
run concurrently.

Global total ordering across a whole topic is out of scope.

## Failure modes at a glance

| What happened | Acked? | Outcome |
|---|---|---|
| Start committed | yes | Execution created. `dispatched` |
| Redelivery of an already-committed message | yes | Same execution returned; no second run. `idempotent_replay` |
| Consumer-group rebalance replays uncommitted offsets | yes, on re-settle | Same as redelivery: dedupe collapses them |
| Crash between commit and ack | no → redelivered | Dedupe collapses the replay onto the original run |
| Start deferred by a throttle (`202`) | yes | The throttle fires it later. `deferred` |
| Body does not decode | yes | Dead-lettered `malformed` |
| Start route rejects it (`4xx`) | yes | Dead-lettered `target_rejected` |
| Mapping function rejects it, under threshold | no → redelivered | `retried` |
| Mapping function rejects it, at threshold | yes | Dead-lettered `mapping_rejected` |
| Transient harvest failure (`5xx`, pool exhausted) | no → redelivered | Never dead-lettered |
| Dead-letter write itself fails | no → redelivered | Downgraded to a retry; the strike count is preserved so the next attempt still dead-letters |
| Broker `receive` errors | n/a | Back off `error_backoff`, poll again |

## Observability

Four metrics, all with bounded labels. Per [ADR-0001 §7](../adr/0001-otel-trace-contract.md)
the message key, offset and execution id are **never** labels:

| Metric | Type | Labels |
|---|---|---|
| `harvest.connector.received` | counter | `source` (binding name) |
| `harvest.connector.dispatched` | counter | `source`, `outcome` ∈ `dispatched` / `idempotent_replay` / `deferred` / `dead_lettered` / `retried` |
| `harvest.connector.poisoned` | counter | `source`, `reason` ∈ `malformed` / `mapping_rejected` / `target_rejected` |
| `harvest.connector.lag` | gauge | `source` — where the broker client exposes it (Kafka high-watermark minus committed offset; SQS `ApproximateNumberOfMessages`) |

A broker-triggered execution also records `start_source = 'broker'` with
`start_source_ref` set to the rendered coordinates, so a run traces back to the
exact message that produced it:

```sql
SELECT id, workflow_name, start_source_ref
FROM harvest_workflow_executions
WHERE start_source = 'broker';
```

Triage a dead-letter backlog with:

```sql
SELECT binding, reason, count(*), max(failed_at)
FROM harvest_connector_dead_letters
GROUP BY binding, reason ORDER BY 3 DESC;
```

## Testing without a broker

The `connectors` feature alone ships `MockSource`, which implements
`EventSource` over an in-memory queue and records what was acked and abandoned.
Drive the real `ConnectorRuntime` against it and every guarantee above — ack
ordering, dedupe, poison isolation, backpressure — is under test with no Docker:

```rust
let source = Arc::new(MockSource::new("orders.placed"));
source.push_kafka(0, 41, br#"{"order_id":"A-1"}"#);
source.push_kafka(0, 41, br#"{"order_id":"A-1"}"#); // deliberate redelivery

let summary = runtime.run_once().await.expect("pass");
assert_eq!(summary.received, 2);
assert_eq!(summary.acked, 2, "both deliveries settle");
assert_eq!(summary.retried, 0);
// ...and the database has exactly one execution for order-A-1.
```

`PassSummary` reports `received` / `acked` / `retried` / `dead_lettered`;
the finer-grained split (`dispatched` vs `idempotent_replay` vs `deferred`)
is on the `harvest.connector.dispatched` counter's `outcome` label, so assert
it with a recording `MetricsRecorder`.

See [`connector_integration.rs`](../../autumn-harvest-plugin/tests/connector_integration.rs)
for the acceptance-criteria suite, including the soak test that pushes 10,000
messages with 5% forced redeliveries and 10 poison messages and asserts exactly
9,990 executions, zero duplicates and 10 dead letters.

## Out of scope

Deliberately not shipped: NATS / RabbitMQ / Pub-Sub / Kinesis adapters (only
`EventSource` is broker-specific, so they are follow-ups rather than rewrites);
outbound event *publishing* (see the transactional outbox in
[chapter 10](10-operations.md)); Kafka exactly-once / transactional semantics;
ordering guarantees beyond the entity pattern above; and schema-registry
(Avro / Protobuf) decoding — the mapping function receives raw bytes, so a
registry client is yours to call.

---

Previous: [Chapter 12: Inbound webhooks](12-webhooks.md)
