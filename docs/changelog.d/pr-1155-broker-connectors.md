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
`harvest_workflow_outbox`, migration `20260719000000`) for poison messages that
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
`topic:partition:offset`, SQS `MessageId` (stable across every redelivery of a
message and distinct for every distinct message; a FIFO
`MessageDeduplicationId` is only a last-resort fallback, see the review-fix
list below) — namespaced by the binding exactly as
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
in flight, nothing commits until 10 lands, then all three commit at once. The
tracker distinguishes *in flight* (delivered to us, not yet settled — blocks the
prefix) from *never delivered* (a hole below the highest offset actually seen —
stepped over), because a partition's delivered offsets are **not** contiguous:
Kafka reserves offsets for transaction control records, filters
aborted-transaction records under `read_committed`, and compaction can remove a
record entirely. Waiting on a hole that will never arrive would stall that
partition's commit permanently — every message processed, but the mark frozen,
so every restart replays all of them. A hole is only stepped over strictly
*below* the highest delivered offset, so the mark can never run ahead into
offsets the broker may still hand us. Kafka
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

`EventSource` therefore has **two** distinct negative-acknowledgement verbs,
because the two intents are opposite: `abandon` is the gentle return of a
message that hit a *transient* harvest failure (SQS lets the visibility timeout
expire naturally, which is also the backoff), while `nack_for_dead_letter` asks
for the *fastest possible* redelivery so the broker's own redrive claims the
message (SQS resets visibility to `0`). Conflating them made the docs' "the
visibility timeout expires" claim false for the transient path. An adapter with
no real nack keeps the default, which routes both to `abandon`; a binding that
asks for `.broker_native_dead_letter()` on such an adapter is **rejected at
build time** (`broker_native_dead_letter_is_supported`), because abandoning
there never terminates — the poison message would be re-read forever and reach
no dead-letter destination at all.

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
— note `dispatched` is the **settlement breakdown**, one sample per received
message, so the series sums to `received` and a dashboard can show the full
disposition mix rather than only the successes —
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

### Further defects the code review found

* **The prefix stalled forever on an offset the broker never delivers.** Kafka
  control records, filtered aborted-transaction records, and compacted-away
  keys all leave holes in a partition's delivered offsets. The tracker waited
  on them, freezing that partition's commit permanently. Fixed by tracking
  in-flight offsets explicitly, so a hole below the highest delivered offset is
  stepped over while a genuinely in-flight offset still blocks.
* **A panicking mapping function was retried forever.** `(binding.mapper)` is
  embedder-supplied code; an `unwrap()` in it unwound the dispatch task, which
  the runtime correctly declined to ack — and then re-read the same message on
  every pass. It is now caught (`catch_unwind`) and classified as `Malformed`,
  which is deterministic and therefore dead-lettered on sight.
* **`ack` committed the message's own offset rather than the advanced mark.**
  The contiguous-prefix computation was correct but its result was discarded,
  so a batch that completed out of order committed a *lower* mark than it had
  earned and re-read the gap on restart.
* **The dead-letter record under-reported its strike count.** `settle` recorded
  a hard-coded attempt count rather than the real consecutive-strike total, so
  an operator triaging a quarantined message could not see how many times it
  had actually been tried.
* **`start_source_ref` recorded the wrong shape on the signal-with-start path.**
  The core resolved it from the idempotency key, which for a
  `signals_with_start` binding is the *namespaced* key rather than the raw
  broker coordinates — so provenance was inconsistent between the two binding
  kinds. `SignalWithStartParams` gained an explicit
  `start_source_ref_override` so both paths record the rendered coordinates
  uniformly.
* **Three docs sites documented the wrong `outcome` label values.** They
  claimed **4** values named `started`/`idempotent_replay`/`deferred`/`retry`;
  the shipped enum has **5**, named `dispatched`/`idempotent_replay`/
  `deferred`/`dead_lettered`/`retried`. Caught by a test that asserts the
  emitted label rather than merely that emission did not panic.
* **SQS coordinated on the wrong id, silently dropping valid events.** The
  adapter preferred a FIFO `MessageDeduplicationId` over the broker-assigned
  `MessageId`, on the reasoning that a producer-controlled id also survives a
  *re-publish*. It is wrong in both directions. Inside SQS's five-minute
  deduplication interval SQS already collapses the re-publish itself, so the
  second message never reaches a consumer and the preference buys nothing.
  Outside it, a legitimately reused dedup id (a nightly job keyed on a business
  id) is a genuinely **new** message with a new `MessageId` — and keying on the
  dedup id collapsed it onto the earlier message's key, so it was acked as an
  idempotent replay and its event dropped. `MessageId` is now the coordinate;
  the dedup id is a last-resort fallback for the case where the broker supplied
  no message id at all.
* **`broker_native_dead_letter()` was accepted on SQS queues with no redrive
  policy.** The adapter reported an unconditional native dead-letter
  destination, but SQS only *has* one when the queue carries a redrive policy.
  Without one, abandoning a poison message redelivers it forever — precisely
  the failure the mode exists to prevent, and now silent rather than caught at
  build time. `SqsSource::connect` now probes `RedrivePolicy` and the answer
  **fails closed**: an undeclared, unprobed, or probe-denied queue reports no
  destination, so the binding is rejected at build time with a message naming
  the two fixes (add a redrive policy, or declare it with
  `SqsSourceConfig::has_redrive_policy(true)` when using the sync
  `SqsSource::new` or when IAM denies `GetQueueAttributes`).
* **A partition reassigned *behind* the mark committed a stale higher offset.**
  An operator resetting the group offset, or a rebalance returning a partition
  another consumer had moved, left the tracker's in-memory mark ahead of the
  broker's position — so completing one low offset reported the old mark and
  committed straight over everything in between. A *fresh* delivery at or below
  the mark is now treated as the reposition it is: the partition's state is
  reset and the prefix rebuilt from the new position. A redelivery of an offset
  still in flight (or completed and held) is not a reposition and leaves the
  live prefix alone.
* **The dead-letter migration would never have run.** Its directory used
  version `20260716000000`, already taken by the core
  `20260716000000_harvest_workflow_history_bloat_warn`. Diesel identifies an
  applied migration by **version alone**, in one `__diesel_schema_migrations`
  table, and `ensure_runtime_migrations` applies the core migrations to the
  harvest database *before* the plugin's — so the core one records the version
  and the plugin's `CREATE TABLE` is treated as already applied and skipped.
  `harvest_connector_dead_letters` would simply not exist, every poison
  message's dead-letter write would fail, and each would be downgraded to a
  retry and redelivered forever: exactly the failure the poison path exists to
  prevent, surfacing far from its cause. Renamed to `20260719000000`.

  Every test passed anyway, because each DB suite creates the table itself
  (`reset()` runs the migration's SQL directly) — so the harness could never
  observe the migration being skipped. The durable fix is therefore a guard,
  not a test of this one table: `migration_hygiene` gained
  `plugin_and_core_migrations_never_share_a_version`, which cross-checks the
  plugin's harvest tree against the core tree. The pre-existing
  `real_migrations_have_unique_version_prefixes` could not catch this — it only
  ever saw the core tree. Verified red against the colliding name.
* **A permanently blocked commit prefix was silent.** The contiguous-prefix
  rule means a message that is retried rather than settled blocks its
  partition's commit. On SQS the visibility timeout resolves that; on Kafka it
  cannot, because `abandon` is a no-op there (not committing is not a nack), so
  nothing hands the message back until the consumer is recreated. The connector
  meanwhile looked healthy — messages flowing, all dispatching, only the commit
  frozen. New `ConnectorRuntimeConfig::stall_threshold`: when a
  partition holds that many completed offsets behind an unsettled head, the
  pass fails with a distinct `ConnectorError::Stalled` carrying the partition,
  the depth and the bound. Checked *before* each receive, both because a
  stalled partition usually goes quiet (so a check that only ran when messages
  arrived would miss the stalls that matter most) and because pulling a batch
  only to drop it on the error is pure churn that advances a positional
  consumer past undispatched messages. **On by default** — a stall nobody
  configured a detector for is exactly the one that goes unnoticed — with the
  bound derived from the binding's `max_in_flight` (×4, floored at 32), which
  is what bounds *healthy* out-of-order settlement. `Some(0)` opts out.

  The first cut only *signalled* the stall: `run`'s generic error arm caught it,
  slept, and re-polled the same wedged consumer forever, and nothing in-tree
  supervises the connector task, so the promised "a supervisor recreates the
  consumer" never happened. Detection without recovery is not a fix, so the
  runtime now performs the retry itself. New defaulted `EventSource::recover()`
  returns whether the source rebuilt its own client; `run` handles `Stalled`
  as its own case (re-polling a wedged consumer accomplishes nothing) by
  calling it and then clearing that partition's tracker state — without which
  the redelivered offsets arrive below the stale mark and the prefix stays
  blocked. Poison strikes are deliberately not cleared, so a repeatedly-rejected
  message still reaches its threshold instead of restarting its count on every
  recovery. `KafkaSource` implements `recover` by rebuilding its consumer
  (held behind a short-lived `Mutex`, never locked across an `.await`), which
  rejoins the group from the last committed offset — genuinely redelivering the
  blocked message. The default is `Ok(false)`, correct for SQS, whose
  visibility timeout already redelivers so its prefix cannot wedge this way; a
  source that both stalls and cannot rebuild itself stops the binding with an
  error rather than spinning.

- **A directly-built runtime silently discarded every poison message.**
  `ConnectorRuntime::new` / `for_binding` are both public, and both installed
  `NoopDeadLetterSink` — while a `SourceBinding` defaults to
  `DeadLetterMode::HarvestSink`. So an embedder wiring the runtime themselves
  (a test harness, a custom supervisor) got "record this poison message" paired
  with a sink that records nothing: the write "succeeded", the runtime
  acknowledged the message, and it was gone with no row in any table and no
  copy on any broker — the one outcome a dead-letter path must never produce.
  `HarvestPlugin` always installs the Postgres sink, so the shipped path was
  never affected. Fixed by making the default sink *fail* rather than silently
  succeed: the new `UnconfiguredDeadLetterSink` returns a `Config` error naming
  both remedies (`with_dead_letter_sink(...)`, or
  `broker_native_dead_letter()`), which drops straight into the runtime's
  existing — and already-tested — ack-only-after-a-durable-write branch, so the
  message is abandoned for redelivery and logged loudly instead of lost. No
  public signature changed. `NoopDeadLetterSink` stays exported for the
  broker-native and test cases, where the sink is genuinely never consulted,
  with its doc updated to say it is no longer the default and why.

- **A `max_batch` of `0` stopped a binding consuming, forever and silently.**
  The value went straight to the source: Kafka's `while batch.len() < max`
  loop never ran and `MockSource` drained nothing, so every pass looked *idle*
  rather than broken — no error, no metric, no log, just a binding that never
  moves again. (SQS happened to survive it, clamping internally.) Floored at
  the runtime's single `receive` call site rather than per adapter, so an
  embedder's own `EventSource` is covered too — new pure
  `effective_max_batch`, mutation-verified: reverting it to the identity makes
  both new tests report `received: 0`.

- **A backward reposition before the first commit skipped every offset in
  between.** `OffsetTracker::observe`'s reset guard was anchored on the
  committed mark, so it was disabled precisely while `committed` was `None` —
  which is the *normal* state of a partition whose head never settles, since
  the prefix cannot advance. Stale `floor`/`ceiling` therefore survived a
  reposition, and `ceiling` is what licenses stepping over an undelivered
  offset as a "broker hole". With offset 10 held in flight and 11 completed, a
  rebalance delivering offset 5 alone would commit through **9** — offsets
  6..=9 were never delivered in that generation and are still to come, so
  committing past them loses them outright. Anchored on `committed.or(floor)`
  instead. Mutation-verified: reverting the anchor makes the new test report
  `Some(9)` where `Some(5)` is required — the loss, exactly.

- **The stall detector could not see a retry at the tail of a quiet
  partition.** `stalled()` fired only once `threshold`-many *later* offsets
  had settled behind the blocked head, so the one case with nothing behind it
  — a transient failure on the last message of an idle partition — was never
  detected, and on Kafka that message is simply dropped (`abandon` is a no-op
  and the read position has already advanced past it). The volume bound was
  always the wrong primary signal; it is now a *backstop* for a head blocked
  without going through the retry path, and the primary signal is the retried
  head itself, recorded on the tracker (`OffsetTracker::retried`) and reported
  immediately.

  Gated on a new `EventSource::abandon_redelivers()` (default `true`, Kafka
  overrides to `false`) rather than on "any retry": SQS's visibility timeout
  genuinely does hand the message back, so firing there would recycle the
  consumer on every transient blip. The capability is explicit because it
  cannot be inferred — both brokers carry partition/offset coordinates, and
  the difference is only in whether `abandon` means anything. `Some(0)` now
  disables **only** the backlog heuristic; the retried head is a correctness
  signal, not a tunable. Mutation-verified in both directions: neutering the
  retried signal makes the tail case report `None` where a stall is required,
  and ignoring the capability makes eight redelivering-source tests fail.

- **Two of the three documented poison `reason` labels matched no series.**
  `docs/telemetry.md` and ADR-0001 advertised `deserialize_failed` /
  `permanent_failure`, but `PoisonReason::as_str()` emits `malformed` /
  `target_rejected`. An operator copying a selector out of those tables would
  build an alert that silently never fires — which reads as "this never
  happens" rather than "you typed the wrong label". Corrected, and pinned by a
  new anti-drift test that enumerates the enum and asserts every emitted value
  appears on each doc surface *and* that neither stale value does, so a future
  variant fails the build until it is documented.

- **A `starts` binding onto a *batched* workflow could deliver one message
  twice.** For a deferred-admission target a keyed start is a `400`, so dedupe
  falls back to workflow-id reuse — and reuse only arbitrates when an execution
  is *created*. Batch admission mutates a pending aggregate long before that:
  `admit_batched_start` upserts `buffered_payloads = existing || EXCLUDED`, so a
  broker redelivery appends the same message a second time and the collapsed run
  sees it twice; it also counts toward `max_size`, so it can flush the batch
  early. Rejected at build time now, with the error naming the remedy (bind to
  an unbatched workflow that starts the batched one, so the connector dedupes on
  coordinates as usual and the batch only ever sees one admission per message).
  The other two deferred policies were checked rather than assumed, and both are
  genuinely safe, so both stay allowed and are documented instead of rejected:
  **debounce** collapses on `(workflow_name, debounce_key)`, so a redelivery
  lands on the same pending row and still yields exactly one run — it costs a
  trailing-edge deadline extension bounded by `max_wait`, and a `pending_count`
  that counts admissions rather than distinct messages; **throttle** keeps one
  row per admission, but each fires through `start_or_load_workflow_execution`,
  where reuse collapses the duplicate and refunds its token.

- **The ordering section promised order the connector does not provide.** The
  entity-pattern paragraph said a workflow "processes its own signals in the
  order they were recorded", which the caveat twenty lines below it already
  contradicted. Two same-key records in one batch are dispatched concurrently
  (`for message in batch { … tokio::spawn(…) }`), so the later offset can
  persist its signal first and the recorded order is not broker order. The
  paragraph now claims **affinity, not ordering**, and the caveat states the
  race explicitly. The one remedy it offers — `.max_in_flight(1)` — is no longer
  taken on faith: `max_in_flight_one_dispatches_in_broker_order` drives twelve
  staggered records through a real dispatch and asserts the observed order, and
  raising the bound to 4 makes it fail (`[1, 2, 0, …]`), so the test measures
  the bound rather than the mock's insertion order.

- **Two bindings could share one event source, losing Kafka messages.**
  `spawn_connectors` builds a `ConnectorRuntime` per registration, so each gets
  its own `OffsetTracker` — handing the same `Arc<dyn EventSource>` to two
  registrations started two receive loops over **one client** with two
  independent views of what is durable. The validator only rejected duplicate
  `(stream, target)` pairs, so same-stream-different-target passed. On Kafka the
  loops split the stream nondeterministically and one tracker could commit a
  contiguous mark covering offsets the *other* loop was still dispatching, which
  a crash then skips permanently; on SQS each message goes to exactly one loop,
  so neither target sees the whole queue. Rejected at build time now, naming
  both bindings and the two remedies that actually fan a stream out (two Kafka
  consumers with distinct group ids, or two SQS queues behind an SNS topic). The
  decision is the pure `first_shared_source`, unit-tested without constructing
  sources — matching `binding_stream_matches_adapter` — and it reports the first
  offending pair so the panic names a stable one rather than whichever the
  iteration order reached.

- **The runnable SQS example still promised per-key ordering.** Its doc comment
  claimed the entity pattern "is also how you get per-key ordering" while
  configuring `.max_in_flight(16)` — the same defect just corrected in the
  guide, left behind in the example an embedder is most likely to copy. It now
  says **affinity, not ordering**, states the same-device race, and points at
  `.max_in_flight(1)` as the explicit throughput trade.

- **A panicked dispatch task left its Kafka offset unmarked.** The normal
  `Retry` path marks a retried head so recovery fires on the head itself, but a
  task lost to a `JoinError` never reaches `settle`, so that marking never ran —
  and the message handle had already moved into the spawned task, so the join
  arm could not reach it. On a source whose `abandon` cannot force a redelivery
  the local position has already advanced past the record, so nothing hands it
  back: the offset became a permanently blocked prefix head, and at the tail of
  a quiet partition the backlog heuristic never fires because nothing settles
  behind it. The positional pair is now captured alongside the join handle and
  the arm marks the head, so the pass fails `Stalled` and the consumer is
  rebuilt. The reproduction is a genuine engine-side panic through the real
  `run_once`: the dead-letter sink is the one injectable seam that panics
  *outside* the mapper's `catch_unwind`. Its assertion uses `threshold == 0`,
  which disables the backlog heuristic, so `held: 0` proves the marked head is
  the only signal that could have fired. A mirror test pins the other
  direction — a redelivering source (SQS's visibility timeout) is not wedged
  and must not recycle the consumer on every engine-side panic.

- **Broker-native dead-lettering restarted its own strike countdown.**
  `AbandonToBrokerDeadLetter` is terminal for harvest but not for the broker: it
  resets visibility so the message returns and the broker counts the receive
  toward its own `maxReceiveCount`. Whenever that ceiling sits above the
  binding's `poison_threshold` — the normal configuration, since the binding
  wants to nack well before the queue gives up — the message comes back one or
  more times before the broker quarantines it, and clearing the strike history
  on the way out sent each redelivery back through ordinary
  visibility-timeout retries and emitted a fresh `dead_lettered` sample per lap.
  The strikes are now kept for that disposition alone, so every later delivery
  re-nacks on sight and the broker's own count is what ends it. Such an entry
  outlives harvest's view of the message, which is what the retention window
  below exists to bound.

- **Consumer lag was measured from the fetch position, not the committed
  offset.** `lag()` subtracted `consumer.position()` from the high watermark,
  but `position()` is the local next-fetch cursor: it advances the instant a
  record is fetched, regardless of whether that record has been dispatched, is
  being retried, or is stuck behind a blocked commit prefix. So a consumer
  wedged with a large uncommitted backlog — precisely the condition
  `harvest.connector.lag` exists to expose — reported its lag collapsing to
  zero while a restart would replay everything from the last commit. It now
  reads the durable committed group offset, falling back to the **low**
  watermark for a partition with nothing committed yet (the consumer is pinned
  to `auto.offset.reset = earliest`, so that is genuinely its outstanding
  backlog; skipping the partition would report zero lag for a consumer that has
  processed nothing). Covered by a broker-suite test that drives the source
  directly so records are fetched but never acked — the honest reproduction of
  a blocked prefix, which read zero under the old computation.

- **A recovered stall rebuilt the consumer with no backoff at all.** The
  `Stalled` arm called `recover_from_stall` and looped straight back, bypassing
  the `error_backoff` every other failure honours. But rebuilding a Kafka
  consumer is not a free local operation: it triggers a **group rebalance**,
  revoking and reassigning partitions across every consumer in the group. A
  stall is almost always caused by a downstream outage (Postgres, an admission
  gate), so the redelivered head fails again immediately — turning one
  binding's outage into a rebalance storm that disrupts unrelated partitions
  and every other consumer in the group, for as long as the outage lasts. The
  arm now applies the same cancellation-aware `error_backoff` the transient
  arm does. The RED measurement is not subtle: a fixture that wedges on every
  pass drove **13,968 consumer rebuilds in 300 ms** (≈ 46 per millisecond)
  before the fix, against a post-fix ceiling of 12 for a 50 ms backoff.

- **Broker-native poison strikes could never be released.** The fix above kept
  a message's strike history through `AbandonToBrokerDeadLetter`, justified at
  the time by "the redrive policy is what bounds it". That reasoning was
  wrong, and the review was right to challenge it: SQS moves the message to its
  DLQ **without notifying this process**, so there is no later delivery and no
  terminal path that can ever clear the key — and a redrive policy bounds
  *deliveries of one message*, not the size of an in-process `HashMap` across a
  sustained stream of *distinct* poison messages. `PoisonTracker` now carries a
  bounded retention for terminal entries: a monotonic-`Instant` FIFO makes
  expiry O(expired) rather than a scan, the window runs from the message's
  **last** delivery (expiring mid-redrive would restart the very countdown
  keeping the strikes avoids), and `MAX_TERMINAL_POISON_ENTRIES` (10 000) is a
  hard cap applied at mark time so a burst between passes cannot outrun the
  bound. Time-based retention alone is still unbounded against a fast enough
  stream; the cap is what makes it hard. Window configurable via
  `ConnectorRuntimeConfig::poison_retention`, defaulting to one hour —
  deliberately generous, because expiring late costs only memory while
  expiring early costs a redelivered poison message a full strike countdown.

### Success metric

> an embedder wires a Kafka topic to a workflow in ≤ 30 lines of
> configuration/mapping code, and a soak test delivering 10,000 messages with
> 5% forced redeliveries and 10 poison messages yields exactly 10,000 − 10
> dispatched outcomes, 0 duplicate executions, and 10 dead-lettered messages.

Both halves are **falsifiable in CI**. The wiring budget is measured, not
claimed: `tests/connector_example_budget.rs` reads the shipped
`examples/kafka_connector_quickstart.rs` and counts the code between its
`connector` markers (**17 lines**; the SQS example is 24), failing if the API ever grows enough
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
