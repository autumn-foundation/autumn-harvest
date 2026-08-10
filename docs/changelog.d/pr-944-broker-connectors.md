## Phase 3.x — Broker event-source connectors: Kafka and SQS as workflow triggers (issue #944)

**Implemented.** A Kafka topic or an SQS queue can now trigger a durable
workflow, with idempotent redelivery, correct ack ordering, poison isolation
and backpressure — none of which the embedder writes. Before this, the only
first-class inbound triggers were HTTP starts and verified webhooks (#344), so
every event-driven deployment hand-rolled a consumer loop and got at least one
of dedupe, ack ordering, or poison handling wrong.

**The core engine gains ZERO broker dependencies.** This is the load-bearing
architectural invariant and the reason the connector lives in
`autumn-harvest-plugin` behind Cargo features rather than in `autumn-harvest`.
The engine stays Postgres-only: `cargo tree -p autumn-harvest --all-features`
never shows `rdkafka`, `aws-sdk-sqs`, or any other broker client. Everything the
engine contributes is dependency-free — four `harvest.connector.*` metric
constants with no-op `MetricsRecorder` defaults, and the additive
`StartSource::Broker` provenance variant. The invariant is enforced
**mechanically**, not by convention:
`autumn-harvest-plugin/tests/connector_dependency_graph.rs` shells out to
`cargo tree` and fails the build if a broker client ever reaches the engine's
graph, plus a falsifiability test proving the guard genuinely *sees*
`rdkafka`/`aws_sdk_sqs` when they are present (so a typo'd crate name or a
broken parse can never make the negative guard pass vacuously). Same seam shape
the engine already uses for `PayloadStore` (#524),
`CompletionCallbackDeliverer` (#605) and `HistoryArchiver` (#345).

**No new `WorkflowEvent` variant, no core migration.** A broker-triggered start
is an ordinary start landing the same `WorkflowStarted` event an HTTP caller
produces. The only durable state the connector owns is
`harvest_connector_dead_letters`, a **plugin**-owned table (alongside
`harvest_workflow_outbox`, migration `20260716000000`) for poison messages that
never became executions — the engine's `harvest_dead_letters` is task-keyed
(`original_task_id NOT NULL` + a `task_type` CHECK), so holding one would have
required relaxing a *core* schema constraint, which the issue forbids for the
trigger path.

### Features and layering

| Feature | Brings |
|---|---|
| `connectors` | The broker-**agnostic** layer: bindings, idempotency, ack ordering, poison isolation, backpressure, and `MockSource`. No broker client at all — enough to unit-test a mapping function and the whole dispatch path with no Docker. |
| `kafka` | `connectors` + `rdkafka`. |
| `sqs` | `connectors` + `aws-config` / `aws-sdk-sqs`. |

Only `EventSource` is broker-specific, so NATS / RabbitMQ / Pub-Sub / Kinesis
adapters are follow-ups rather than rewrites. New module tree
`autumn-harvest-plugin/src/connector/`: `binding.rs` (the descriptor),
`idempotency.rs` (the injective bounded dedupe key), `disposition.rs` (the
**pure** ack/retry/dead-letter decision core), `dispatch.rs` (delegates to the
plugin's own start handlers), `runtime.rs`, `dead_letter.rs`, `message.rs`,
`source.rs`, `mock.rs`, plus the feature-gated `kafka.rs` / `sqs.rs`.

### Declarative binding (AC2)

`SourceBinding::starts(name, stream, workflow)` /
`::signals_with_start(name, stream, workflow, signal_name)` mirror
`WebhookTriggerInfo`'s shape: a **synchronous** mapping function returning a
`MappedMessage` (workflow id + JSON payload), with `MappingError::Deserialize`
vs `::Rejected` classification exactly as the `#[webhook]` dispatch shim does.
`SignalsWithStart` reuses `signal_with_start_workflow_execution` (#244).
`HarvestPlugin::connector(binding, source)` wires it; `HarvestPlugin::build`
**fails fast** on an unregistered target, a DAG target, a duplicate binding
name, a missing mapping function, an adapter/binding stream mismatch, or a
`BrokerCoordinates` dedupe mode explicitly forced onto a deferred-admission
workflow.

### Idempotency by construction (AC3)

The dedupe key is derived from stable broker coordinates — Kafka
`topic:partition:offset`, SQS `MessageDeduplicationId` (FIFO,
producer-controlled so it survives a *re-publish*) falling back to `MessageId`
(the honest limit of a standard queue) — namespaced by the binding exactly as
the webhook receiver namespaces `{path}:{signal_name}:{delivery_id}`, and fed
to harvest's own `idempotency_key` machinery (#808). The encoding is the
injective, length-bounded `L{len}:{value}` / `H{64hex}` scheme from #699, so a
pathological coordinate can neither collide with another message's key nor blow
the column limit.

**AC3 × AC5 conflict, resolved:** a broker-coordinate-keyed start is mutually
exclusive with a throttle/debounce/batch admission policy (the start route
returns `400`, since a deferred start has no execution id to key). The
connector therefore resolves an `IdempotencyMode` **once at build time**: a
target with a deferred-admission policy falls back to `WorkflowId` dedupe (the
mapping function's id is the dedupe unit, resolved through the normal id-reuse
policy), everything else uses `BrokerCoordinates`. `.idempotency_mode(...)`
forces one explicitly; forcing the incompatible combination is a build-time
panic, not a first-message surprise. `SignalsWithStart` always uses broker
coordinates — the signal path's key is a body field with no such exclusion.

### At-least-once with correct ack ordering (AC4)

Ack/commit/delete happens **only** after the dispatch durably committed, was
recognised as an idempotent replay, or was deferred by the throttle. A
harvest-side failure leaves the message unacked. For Kafka there is one extra
rule, because an offset commit is a **high-water mark**: committing `N` asserts
everything below `N` is done, so the runtime commits only the **contiguous
completed prefix** via `OffsetTracker` — if 11 and 12 finish while 10 is still
in flight, nothing commits until 10 lands, then all three commit at once. Kafka
auto-commit is force-disabled *last* in `to_client_config()` so a caller-supplied
`extra` property can never re-enable it and silently break the contract.

### Backpressure composes with the throttle (AC5)

`max_in_flight` (default `16`, per binding) bounds concurrent dispatches. A
throttle-deferred start (`202`, no execution id) counts as a **successful**
dispatch and is acked — busy-retrying it would defeat the throttle and stampede
the admission path.

### Poison isolation (AC6)

Deterministic failures (undecodable body, a `4xx` target rejection) dead-letter
immediately; a mapping-function *rejection* is retried until
`poison_threshold` consecutive strikes (default `3`, mirroring
`poison_pill_threshold` #367) then dead-letters. `poison_threshold(0)` disables
the strike counter but **not** deterministic dead-lettering, because retrying an
undecodable body forever is exactly the partition wedge this prevents. A
transient harvest failure (`5xx`, pool exhausted) is **never** dead-lettered no
matter how often it recurs. Destination is either the harvest-side table (with
the raw payload, so an operator can replay by hand after a fix; `idempotency_key`
is `UNIQUE`, so dead-lettering is itself idempotent) or —
`.broker_native_dead_letter()` — the broker's own machinery (SQS redrive: the
visibility timeout is reset to `0`, so SQS re-delivers, counts the receive, and
moves the message to the queue's DLQ once `maxReceiveCount` is hit).

### Ordering caveat, documented honestly (AC7)

The runtime dispatches up to `max_in_flight` messages concurrently and does
**not** preserve broker partition ordering. Per-key ordering is the entity
pattern: a `signals_with_start` binding whose mapping function derives a stable
`workflow_id` from the partition key routes every message for that key into the
same execution, and a workflow processes its own signals in recorded order.
Global total ordering across a topic is out of scope.

### Observability (AC8)

Four metrics via the three-touchpoint recipe, all with **bounded** labels —
per ADR-0001 §7 the message key, offset and execution id are never labels:
`harvest.connector.received{source}`,
`harvest.connector.dispatched{source, outcome}` (`dispatched` /
`idempotent_replay` / `deferred` / `dead_lettered` / `retried`),
`harvest.connector.poisoned{source, reason}` (`malformed` /
`mapping_rejected` / `target_rejected`), and `harvest.connector.lag{source}`
where the client exposes it (Kafka high-watermark minus committed offset; SQS
`ApproximateNumberOfMessages`). A broker-triggered execution also records
`start_source = 'broker'` with `start_source_ref` set to the rendered
coordinates, so a run traces back to the exact message.

### Two bugs the new tests found

* A dead-letter **sink write failure** was counted as a poisoned message even
  though the message was actually abandoned for redelivery. `settle` now
  returns the *effective* disposition (downgrading `DeadLetter` → `Retry`) and
  both the metrics emission and the poison-strike clear follow it, so the
  counters can never over-report a quarantine that did not happen.
* `OffsetTracker::complete` withheld the ack for a **redelivered offset at or
  below** the committed high-water mark (the naive prefix advance returns
  `None` for it), so a replayed message would be redelivered forever. It now
  reports the existing mark — a commit is idempotent — without ever advancing
  past an in-flight gap.

### Success metric

> an embedder wires a Kafka topic to a workflow in ≤ 30 lines of
> configuration/mapping code, and a soak test delivering 10,000 messages with
> 5% forced redeliveries and 10 poison messages yields exactly 10,000 − 10
> dispatched outcomes, 0 duplicate executions, and 10 dead-lettered messages.

Both halves are **falsifiable in CI**. The wiring budget is measured, not
claimed: `tests/connector_example_budget.rs` reads the shipped
`examples/kafka_connector_quickstart.rs` and counts the code between its
`connector` markers (**18 lines**), failing if the API ever grows enough
boilerplate to exceed 30 — with a companion test asserting the marked block
still contains a real binding, mapping function and source, so the budget
cannot be met by an example that stopped demonstrating the thing. The soak is
`connector_integration.rs::soak_ten_thousand_messages_with_redeliveries_and_poison`,
run against a real Postgres: **10,500 deliveries settled in 21.6 s**, exactly
9,990 executions, zero duplicates, 10 dead letters.

### Tests

* **88 unit tests** across the connector modules — the pure `disposition.rs`
  decision core (ack ordering, throttle composition, poison isolation, the
  contiguous-prefix `OffsetTracker` including the redelivery and
  in-flight-gap cases), `idempotency.rs` (injectivity, separator-collision and
  pathological-length cases), `binding.rs`, `runtime.rs`, `mock.rs`, `kafka.rs`,
  `sqs.rs`.
* **`connector_integration.rs`** — 7 AC-mapped tests plus the soak, driven
  through the real `ConnectorRuntime` against a real Postgres so the ack
  ordering, poison accounting and backpressure under test are production code
  paths: redelivery dedupe (start and signal), crash-between-commit-and-ack,
  rejected-dispatch-never-acked, throttle-deferral acked, poison isolation
  (1 poison + 100 valid → all 100 dispatch), broker provenance.
* **`connector_kafka_broker.rs` / `connector_sqs_broker.rs` (AC11)** — the
  adapters against **real broker containers** (testcontainers Kafka;
  ElasticMQ for SQS, which speaks the SQS query API and starts far faster than
  LocalStack for a queue-only test). These prove the wire behaviour the
  `MockSource` suite cannot: that an offset commit really commits (a fresh
  consumer in the same group re-reads nothing — a no-op `ack` would re-read
  everything), that an SQS ack really is a `DeleteMessage`, and that an
  abandoned poison message is redelivered, quarantined, and never blocks its
  valid sibling.
* **`connector_dependency_graph.rs`** — the AC1 invariant guard described above.

### CI

Manifest rows for `connector_integration` (linux, `connectors`),
`connector_sqs_broker` (linux, `sqs`), and the two feature-free guards (allos);
clippy legs for `connectors` / `sqs` / `kafka`; and Linux-only steps for the
Kafka broker suite and example (they need a `libcurl4-openssl-dev` install for
vendored librdkafka's cmake build, and the manifest's compile mode would try to
build it on macOS/Windows too — hence the single, reasoned `ALLOWLIST` entry
pointing at those steps).

### Docs

`docs/getting-started/13-broker-connectors.md` — full Kafka and SQS examples,
the ack-ordering contract, the idempotency table, the failure-mode table
(redelivery, rebalance, poison, throttle-deferral, transient failure,
dead-letter-write failure), the ordering caveat, the metric table, and how to
test the whole thing with `MockSource` and no Docker. Runnable examples:
`examples/kafka_connector_quickstart.rs` and
`examples/sqs_connector_quickstart.rs`.

### Out of scope (per the issue)

NATS / RabbitMQ / Pub-Sub / Kinesis connectors; outbound event publishing;
Kafka exactly-once / transactional semantics; ordering guarantees beyond the
entity pattern; schema-registry (Avro / Protobuf) decoding — the mapping
function receives raw bytes; and any change to core storage.
