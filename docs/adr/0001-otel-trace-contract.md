# ADR 0001 — OpenTelemetry Trace Propagation Contract

**Status**: Accepted  
**Date**: 2026-05-01  
**Issue**: [#93](https://github.com/autumn-foundation/autumn-harvest/issues/93)

---

## Context

The telemetry scaffolding (`TraceContextCarrier`, `TraceContextPropagator`,
`MetricsRecorder`, `TelemetryConfig`) shipped in `telemetry.rs` with no-op
defaults and no contract defining *which spans* Harvest emits, *which attributes*
operators can rely on, or *how trace context interacts with replay*. Without this
contract, downstream apps cannot wire existing OTel pipelines into Harvest with
confidence. A trace that begins at the autumn-web HTTP handler vanishes the
moment it crosses into a workflow, leaving the on-call operator to grep event
JSON across shards.

This ADR defines the stable, versioned contract for:

- Every span Harvest emits (name, kind, parent rules, required attributes)
- Trace context propagation across every workflow boundary
- Replay semantics for spans
- Where trace context is durably stored and where it is explicitly **not**
- Metric names and cardinality constraints

---

## Decision

### 1. Span attribute schema

The following constants are defined in `telemetry.rs` and must be used
verbatim on every span Harvest emits. The concrete Rust constants are the
source of truth — the strings below repeat them for documentation clarity.

| Rust constant          | Attribute key              | Type      | Cardinality          | Notes                                                  |
|------------------------|----------------------------|-----------|----------------------|--------------------------------------------------------|
| `ATTR_WORKFLOW_ID`     | `harvest.workflow.id`      | `string`  | bounded by reg.      | Logical workflow name (e.g. `"onboarding"`)            |
| `ATTR_EXECUTION_ID`    | `harvest.execution.id`     | `string`  | unbounded — no label | UUID of this specific run; see §7 for label guidance   |
| `ATTR_SHARD_ID`        | `harvest.shard.id`         | `int`     | bounded (≤ 256)      | Shard number encoding in execution UUID                |
| `ATTR_ACTIVITY_NAME`   | `harvest.activity.name`    | `string`  | bounded by reg.      | Activity function name (e.g. `"send_email"`)           |
| `ATTR_ATTEMPT`         | `harvest.attempt`          | `int`     | bounded (≤ max_retry)| 1-based attempt number                                 |
| `ATTR_QUEUE`           | `harvest.queue`            | `string`  | bounded by config    | Task queue name                                        |
| `ATTR_REPLAY`          | `harvest.replay`           | `bool`    | 2 values             | `true` on spans emitted during deterministic replay    |

**OTel semantic convention alignment**: Harvest does not yet have upstream OTel
semconv entries. Until registered, the `harvest.*` namespace is used to avoid
conflicts with `workflow.*` (reserved by OTel SIG-Workflow) and `db.*`.

---

### 2. Span catalogue

Every span Harvest emits is listed below.  Spans using the `db` feature are
only emitted when the `db` Cargo feature is active (the default).

#### 2.1 `harvest.workflow.execute` — workflow executor cycle

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `INTERNAL`                                                |
| Parent          | Restored from `harvest_task_queue.trace_context` carrier  |
| Attributes      | `harvest.workflow.id`, `harvest.execution.id`, `harvest.shard.id`, `harvest.queue`, `harvest.replay` |
| On replay       | New root span (no parent); `harvest.replay = true`; link to original (§4) |
| End condition   | After executor cycle completes or suspends                |

#### 2.2 `harvest.activity.execute` — activity invocation

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `INTERNAL`                                                |
| Parent          | The enclosing `harvest.workflow.execute` span             |
| Attributes      | `harvest.activity.name`, `harvest.execution.id`, `harvest.attempt`, `harvest.queue` |
| On replay       | Omitted — activities are not re-executed during replay    |

#### 2.3 `harvest.workflow.schedule` — workflow start / enqueue

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `PRODUCER`                                                |
| Parent          | Active span at call site (the HTTP handler span, etc.)    |
| Attributes      | `harvest.workflow.id`, `harvest.execution.id`, `harvest.shard.id`, `harvest.queue` |
| Notes           | Created when a caller starts a workflow; the `traceparent` of this span is captured and stored on `harvest_task_queue.trace_context` to be restored by the worker (§3) |

#### 2.4 `harvest.activity.schedule` — activity enqueue

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `PRODUCER`                                                |
| Parent          | The enclosing `harvest.workflow.execute` span             |
| Attributes      | `harvest.activity.name`, `harvest.execution.id`, `harvest.queue` |

#### 2.5 `harvest.signal.send` — signal delivery

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `PRODUCER`                                                |
| Parent          | Active span at call site                                  |
| Attributes      | `harvest.workflow.id`, `harvest.execution.id`             |

#### 2.6 `harvest.signal.deliver` — signal received by workflow

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `CONSUMER`                                                |
| Parent          | Restored from signal row's `trace_context` column         |
| Attributes      | `harvest.workflow.id`, `harvest.execution.id`             |

#### 2.7 `harvest.timer.fire` — durable timer expiry

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `INTERNAL`                                                |
| Parent          | Enclosing `harvest.workflow.execute` span                 |
| Attributes      | `harvest.execution.id`                                    |

#### 2.8 `harvest.child_workflow.start` — child workflow invocation

| Property        | Value                                                     |
|-----------------|-----------------------------------------------------------|
| Kind            | `PRODUCER`                                                |
| Parent          | Enclosing `harvest.workflow.execute` span of the parent   |
| Attributes      | `harvest.workflow.id` (child), `harvest.execution.id` (child), `harvest.shard.id` (child) |

---

### 3. Trace context propagation boundaries

The following table describes exactly how context travels at each boundary.

| Boundary                      | How context travels                                             | Notes                                                      |
|-------------------------------|-----------------------------------------------------------------|------------------------------------------------------------|
| HTTP handler → workflow start | `TraceContextPropagator::capture()` at `start_workflow` call site; stored on `harvest_task_queue.trace_context` (JSONB) | Carried via `TraceContextCarrier`                         |
| Worker task pop → workflow execute | `TraceContextPropagator::install(carrier)` before invoking handler; guard dropped after executor cycle | Returns `Box<dyn Any + Send>` so it survives `.await`     |
| Workflow → activity enqueue   | `TraceContextPropagator::capture()` inside executor; new carrier stored on the activity's task row | Captures the `harvest.workflow.execute` span context       |
| Worker task pop → activity execute | Same as workflow execute path                              |                                                            |
| Workflow → child workflow     | Caller captures context at `ctx.start_child_workflow()` call   | Same pattern as HTTP handler → workflow start              |
| Signal send → signal deliver  | Caller captures context; stored on `harvest_signals.trace_context` column | Transient; deleted after delivery                         |
| Timer fire                    | No context propagation; timer fires inside the workflow executor cycle span | Inherits parent span from workflow executor               |

---

### 4. Replay semantics

**The problem**: A workflow can be replayed 24 hours after original execution.
The original trace in the APM collector has long since expired. If replay naively
calls `install(original_carrier)`, the worker emits spans parented to a
non-existent trace — silently broken linkage.

**The contract**:

1. Before a workflow task is popped for replay, the worker converts the carrier:
   ```rust
   let live_carrier = /* loaded from harvest_task_queue.trace_context */;
   let replay_carrier = live_carrier.into_replay_context();
   // replay_carrier.traceparent == None
   // replay_carrier.is_replay   == true
   // replay_carrier.link_traceparent == Some(original_traceparent)
   ```

2. The worker detects `carrier.is_replay == true` and **does not** call
   `propagator.install(carrier)` for parent context. Instead:
   - It creates a new root span.
   - It attaches an OTel span *link* referencing `link_traceparent` (best-effort;
     skipped gracefully if the propagator doesn't support span links).
   - It sets `harvest.replay = true` on the span.

3. Activity spans are **not emitted** during replay — activities are not
   re-executed, only their recorded results are replayed.

**Worked example (24-hour replay)**:

```
T=0h (original execution)
  [HTTP span] POST /api/start
    └─ [harvest.workflow.schedule] harvest.workflow.id="onboarding"
         traceparent stored on task row

T=0h (worker pops task)
  [harvest.workflow.execute] harvest.workflow.id="onboarding"
    harvest.replay=false, parent=<HTTP span>
    └─ [harvest.activity.schedule] harvest.activity.name="send_welcome"
    └─ [harvest.activity.execute] harvest.activity.name="send_welcome"

T=24h (replay after crash recovery)
  [harvest.workflow.execute] harvest.workflow.id="onboarding"   ← NEW ROOT SPAN
    harvest.replay=true
    links=[<original traceparent from T=0h>]     ← link, not parent
    (no activity spans — replay replays results, does not re-execute)
```

---

### 5. Trace context storage rules

| Location                           | Allowed | Rationale                                                             |
|------------------------------------|---------|-----------------------------------------------------------------------|
| `harvest_task_queue.trace_context` | **YES** | Transient; row is deleted after task completion. Replay can convert carrier with `into_replay_context()` before installing. |
| `harvest_signals.trace_context`    | **YES** | Transient; row is deleted after signal delivery.                      |
| `harvest_workflow_executions`      | optional| An opaque `initial_traceparent` column may be added for observability. Not required by this ADR. |
| `harvest_events.event_data`        | **NO**  | Append-only invariant must not be compromised. Trace context is transient by nature; storing it in the event log would permanently couple the event history to a potentially-expired trace. |

The `TraceContextCarrier` struct is the sole wire format for all allowed
locations. It serialises to JSONB via `serde_json`.

---

### 6. No-op contract

When `TelemetryConfig::default()` is used (the default when no telemetry is
configured):

- `NoOpPropagator::capture()` returns `None` — no carrier is written to task
  rows; the `trace_context` column is left `NULL`.
- `NoOpPropagator::install()` is never called on `NULL` rows — zero context
  restoration work.
- `NoOpMetrics` discards every sample with default no-op method bodies; the
  compiler eliminates these calls.
- **Zero span allocations** on the hot path beyond today's baseline.
- The `ATTR_*` and `METRIC_*` constants are compile-time `&str` slices — zero
  run-time cost whether or not telemetry is configured.

---

### 7. Metric catalogue

> **Note:** the table below is a historical snapshot. The authoritative
> catalogue is the set of `METRIC_*` constants in `telemetry.rs`, machine-
> enforced (100% dashboard coverage) by
> `autumn-harvest/tests/integration/dashboard_pack_docs.rs` and visualized by
> `docs/dashboards/starter-pack-v0.1.0.json`.

The following metrics are defined by the constants in `telemetry.rs`. The
`MetricsRecorder` trait method that drives each metric is listed alongside it.

| Rust constant              | Metric name                   | Instrument   | Labels (cardinality)                          | Forbidden labels      |
|----------------------------|-------------------------------|--------------|-----------------------------------------------|-----------------------|
| `METRIC_WORKFLOW_STARTED`  | `harvest.workflow.started`    | Counter      | `workflow.name` (bounded), `queue` (bounded)  | `execution.id`        |
| `METRIC_WORKFLOW_DURATION` | `harvest.workflow.duration`   | Histogram    | `workflow.name`, `queue`, `status`            | `execution.id`        |
| `METRIC_WORKFLOW_TERMINAL` | `harvest.workflow.terminal`   | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue`, `outcome` (6 values: completed/failed/cancelled/timed_out/terminated/continued_as_new) | `execution.id` |
| `METRIC_WORKFLOW_TIMEOUT`  | `harvest.workflow.timeout`    | Counter      | `workflow` (bounded), `queue` (bounded)       | `execution.id`        |
| `METRIC_WORKFLOW_CHAIN_TIMEOUT` | `harvest.workflow.chain_timeout` | Counter | `workflow` (bounded), `queue` (bounded) — fires when a continue-as-new chain outlives its `chain_execution_timeout` cap (issue #617); distinct from `harvest.workflow.timeout` | `execution.id` |
| `METRIC_WORKFLOW_HISTORY_BLOAT` | `harvest.workflow.history_bloat` | Counter | `workflow` (= `METRIC_LABEL_WORKFLOW`, bounded) — **at-least-once** delivery: fires the first time an execution's recorded `harvest_events` count crosses `history_bloat_warn_fraction * event_hard_cap` (`WorkflowHistoryPolicy`, issue #704, default fraction `0.75`), stamping a durable per-execution guard column (`history_bloat_warned_at`) so a later decision cycle never re-fires for the same crossing — except a rare crash-window duplicate (the counter is emitted BEFORE the guard is durably marked, deliberately biased toward a rare duplicate over a lost-forever signal; see `emit_history_bloat_warning_if_crossed`). Two emission paths, not one: (1) a still-**RUNNING** (non-terminal) execution mid-decision — an observation-only early warning, the run keeps executing normally; and (2) the SAME decision that crosses the threshold ALSO reaches the event-count hard cap in one inline append batch and terminally fails the execution (`fail_workflow_for_history_cap`) — the counter still fires there too, based on the durable post-failure event count (never a prospective, not-yet-persisted one). A no-op (permanently flat series) for any workflow type with no `event_hard_cap` configured. Distinct from `harvest.workflow.history_oversized` (a periodically-**sampled gauge** driven by the separate, fleet-wide `HarvestBuilder::max_workflow_history_events` ceiling, issue #493) | `execution.id` |
| `METRIC_ACTIVITY_DURATION` | `harvest.activity.duration`   | Histogram    | `activity.name` (bounded), `queue`, `status` (`completed\|failed`) | `execution.id`, `activity.id` |
| `METRIC_ACTIVITY_FAILED`   | `harvest.activity.failed`     | Counter      | `activity` (bounded), `workflow.type`, `error.type` (low-cardinality), `non_retryable` | `execution.id`, `activity.id` |
| `METRIC_ACTIVITY_ATTEMPTS` | `harvest.activity.attempts`   | Counter      | `activity` (bounded), `queue` (bounded), `outcome` (`completed\|failed`) | `execution.id`, `activity.id` |
| `METRIC_ACTIVITY_RETRIES`  | `harvest.activity.retries`    | Counter      | `activity` (bounded), `queue` (bounded)       | `execution.id`, `activity.id` |
| `METRIC_TIMER_STARTED`     | `harvest.timer.started`       | Counter      | _(none)_                                      |                       |
| `METRIC_QUEUE_DEPTH`       | `harvest.queue.depth`         | Gauge        | `queue` (bounded)                             | `execution.id`        |
| `METRIC_WORKFLOW_ACTIVE`   | `harvest.workflow.active`     | Gauge        | `workflow` (bounded), `state` (2 values: running/paused) | `execution.id` |
| `METRIC_DLQ_ENTRIES`       | `harvest.dlq.entries`         | Gauge        | `shard` (≤ 256)                               |                       |
| `METRIC_QUEUE_PAUSED`      | `harvest.queue.paused`        | Gauge        | `queue` (bounded)                             | 1 = operator hold in effect (issue #619) |
| `METRIC_SCHEDULE_RUNS`     | `harvest.schedule.runs`       | Counter      | `kind` (2 values), `name` (bounded)           |                       |
| `METRIC_SCHEDULE_SKIPPED`  | `harvest.schedule.skipped`    | Counter      | `kind`, `name`, `reason` (3 values)           |                       |
| `METRIC_SCHEDULE_OVERDUE`  | `harvest.schedule.overdue`    | Gauge        | `kind` (2 values), `name` (bounded)           | `execution.id`        |
| `METRIC_RETENTION_DELETED` | `harvest.retention.deleted`   | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`)        |                       |
| `METRIC_SCANNER_TICK`      | `harvest.scanner.tick`        | Counter      | `scanner` (bounded, 7 values: timeout/sla/poison_pill/external_outbox/retention/schedule/pause_auto_resume; 7 labels but 5 spawned loops — `sla`/`external_outbox` are ticked by the `timeout` loop that drives them, so those three cannot diverge) | `execution.id`, `shard` (a multi-shard worker runs one loop per shard under one label; per-shard localisation is the `scanner_liveness` preflight check's job, not the counter's: it reports the worst instance and names its shard) |
| `METRIC_WORKFLOW_PAUSED`   | `harvest.workflow.paused`     | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` | `execution.id`      |
| `METRIC_WORKFLOW_PAUSE_DURATION` | `harvest.workflow.pause_duration` | Histogram | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` | `execution.id` |
| `METRIC_SAGA_COMPENSATED`  | `harvest.saga.compensated`    | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` | `execution.id` |
| `METRIC_SAGA_COMPENSATION_FAILED` | `harvest.saga.compensation_failed` | Counter | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` | `execution.id` |
| `METRIC_ACTIVITY_PANIC`    | `harvest.activity.panic`      | Counter      | `activity` (= `METRIC_LABEL_ACTIVITY`, bounded), `queue` (bounded) | `execution.id`, `activity.id` |
| `METRIC_WORKFLOW_PANIC`    | `harvest.workflow.panic`      | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` | `execution.id`        |
| `METRIC_SIGNAL_RECEIVED`   | `harvest.signal.received`     | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` — **no `name` label**: signal names come from the free-form send route `POST /workflows/{id}/signal/{signal_name}` and have no declared registry to bound them (issue #684, Codex P2) | `execution.id`, signal `name` |
| `METRIC_SIGNAL_UNHANDLED`  | `harvest.signal.unhandled`    | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` — **no `name` label** (same free-form-send-route reason as `harvest.signal.received`; the worker sums the terminal outcome's per-name unconsumed map into the single `(workflow, queue)` series). **graceful terminals only** (`Completed`/`Failed` reached through the workflow drive); forced-failure / scanner terminal paths (`TIMED_OUT`/`CANCELLED`/`TERMINATED`/parent-close cascade/history-cap failure) have no driven matcher and are NOT counted | `execution.id`, signal `name` |
| `METRIC_UPDATE_ADMITTED`   | `harvest.update.admitted`     | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue` — **no `name` label**: admission happens at the free-form update route `POST /workflows/{id}/update/{name}` before the name is resolved against a handler, and handlers register both declaratively AND imperatively (`ctx.register_update_handler`, unknown until execution), so the name cannot be bounded by construction; per-name visibility lives on `update.completed`/`failed`/`rejected` (issue #684, Codex P2) | `execution.id`, update `name` |
| `METRIC_UPDATE_REJECTED`   | `harvest.update.rejected`     | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `name` (update name, bounded) | `execution.id` |
| `METRIC_UPDATE_COMPLETED`  | `harvest.update.completed`    | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `name` (update name, bounded), `queue` | `execution.id` |
| `METRIC_UPDATE_FAILED`     | `harvest.update.failed`       | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `name` (update name, bounded), `queue` | `execution.id` |
| `METRIC_UPDATE_DURATION`   | `harvest.update.duration`     | Histogram    | `workflow` (= `METRIC_LABEL_WORKFLOW`), `name` (update name, bounded — `__unregistered__` for an unresolved name), `queue`, `outcome` (`completed`/`failed`, bounded — rejected excluded) | `execution.id`, `update_id` |
| `METRIC_CONNECTOR_RECEIVED` | `harvest.connector.received` | Counter      | `source` (= `METRIC_LABEL_SOURCE`, the binding's registered `source_name` — a closed set fixed at build time) | `execution.id`, message key, partition, offset, `MessageId` |
| `METRIC_CONNECTOR_DISPATCHED` | `harvest.connector.dispatched` | Counter   | `source`, `outcome` (5 values: dispatched/idempotent_replay/deferred/dead_lettered/retried — one sample per received message, so the series sums to `harvest.connector.received`) | `execution.id`, message key, partition, offset |
| `METRIC_CONNECTOR_POISONED` | `harvest.connector.poisoned` | Counter      | `source`, `reason` (3 values: malformed/mapping_rejected/target_rejected) | `execution.id`, message key, partition, offset |
| `METRIC_CONNECTOR_LAG`     | `harvest.connector.lag`       | Gauge        | `source` — only emitted for sources whose broker client exposes lag (Kafka, SQS); others never emit | `execution.id`, partition |

**Broker-connector labels (issue #944).** A broker message's own coordinates —
Kafka `{topic}:{partition}:{offset}`, an SQS `MessageId`, or a partition key —
are **unbounded by construction** (one series per message) and are therefore
never metric labels; they belong on the dead-letter record and in logs. The
only per-message dimension exposed is the binding's `source` name, which is a
closed set fixed at build time by the registered `SourceBinding`s. Per-message
provenance is recoverable from the started execution's `start_source_ref`
(issue #740) and from `harvest_connector_dead_letters`.

**Cardinality rule**: `execution.id` (a UUID) is **explicitly forbidden** as a
metric label. It is unbounded and would explode the metric time-series in any
production APM. Use `ATTR_EXECUTION_ID` only on *spans*, never on *metrics*.

**Rate-limit throttle counter — label change (issue #699, BREAKING for
operators).** `harvest.rate_limit.throttled` is now labelled by the **bounded
activity name** (`activity`), not the raw bucket `key`. Per-key rate limits
(#699) resolve a bucket key from workflow input at enqueue time
(`dyn-rate:{expr}:{tenant}`), which embeds unbounded tenant input; using it as a
metric label would create one time-series per tenant forever. The counter is
therefore labelled by the registered activity name, which is bounded. **Any
dashboard or alert keyed on the old `key` label of `harvest_rate_limit_throttled`
must move to `activity`.** For the same reason, the per-key token/refill
**gauges** (`harvest.rate_limit.tokens_available` / `harvest.rate_limit.refill_rate`)
exclude the unbounded `dyn-rate:` and `start-throttle:` bucket families from their
per-key sampler; per-tenant bucket state is observable via
`GET /admin/rate-limits`, not metrics.

---

### 8. Worked example — full end-to-end trace

An autumn-web HTTP handler starts `onboarding`. Two activities run on different
workers. `send_welcome` fails once and retries.

```
[HTTP span] POST /api/onboarding  (trace-id: 0af7651916cd43dd)
  │
  └─ [harvest.workflow.schedule] PRODUCER
       harvest.workflow.id = "onboarding"
       harvest.execution.id = "exec-uuid-1"
       harvest.shard.id = 0
       harvest.queue = "default"
       ← traceparent captured here, stored on task row
       │
       ╔══════════════ (Postgres task queue boundary) ══════════════╗
       │
       └─ [harvest.workflow.execute] INTERNAL  (Worker-A)
            harvest.workflow.id = "onboarding"
            harvest.execution.id = "exec-uuid-1"
            harvest.replay = false
            │
            ├─ [harvest.activity.schedule] PRODUCER
            │    harvest.activity.name = "send_welcome"
            │    harvest.attempt = 1
            │    harvest.queue = "email-workers"
            │    ← traceparent captured, stored on activity task row
            │    │
            │    ╔═════ (Postgres task queue boundary) ═════╗
            │    │
            │    └─ [harvest.activity.execute] INTERNAL  (Worker-B)
            │         harvest.activity.name = "send_welcome"
            │         harvest.attempt = 1
            │         harvest.queue = "email-workers"
            │         status: FAILED → retry scheduled
            │    │
            │    └─ [harvest.activity.execute] INTERNAL  (Worker-B, attempt 2)
            │         harvest.activity.name = "send_welcome"
            │         harvest.attempt = 2
            │         status: OK
            │
            └─ [harvest.activity.schedule] PRODUCER
                 harvest.activity.name = "update_crm"
                 harvest.attempt = 1
                 harvest.queue = "default"
                 │
                 └─ [harvest.activity.execute] INTERNAL  (Worker-A)
                      harvest.activity.name = "update_crm"
                      harvest.attempt = 1
                      status: OK
```

---

### 9. Out of scope

The following are **explicitly not** covered by this ADR:

- **Implementation**: span emission code in `worker.rs`, `executor.rs`,
  `queue.rs`. This is the next ticket (implementation will be **M** complexity).
- **New `WorkflowEvent` variants**: trace context never enters the event log.
- **Vendor-specific exporters** (Jaeger, Honeycomb, Datadog, Tempo). The OTel
  Rust SDK is the boundary; exporters are the application's concern.
- **Log correlation** with `tracing` log records — separate spec.
- **Custom user-defined spans** inside `#[activity]` bodies. These are additive
  and the application's responsibility.
- **Cross-language trace propagation**: W3C `traceparent` is the wire format and
  implicitly handles this, but no Rust-SDK-level guarantees are made.
- **Profiling, exemplars, continuous profiling**.
- **Cross-worker sticky routing** for trace continuity across workers.

---

### 10. Consequences

**Positive**:

- Operators can trace an HTTP request end-to-end from autumn-web through any
  number of activities and child workflows using their existing APM tool.
- The append-only `harvest_events` invariant is preserved — trace context never
  enters the event log.
- The replay footgun (parenting a replay span to a long-expired trace) is
  eliminated at the data-model level: `into_replay_context()` strips the parent
  and sets `is_replay = true` before the worker can call `install`.
- The no-op contract (zero allocations on unconfigured path) is preserved.
- `execution.id` cardinality danger is explicitly called out — metric cardinality
  budget is protected from day one.

**Negative / trade-offs**:

- An extra nullable `trace_context` column on `harvest_task_queue` is required
  (one migration). This is a transient table; the column is cheap.
- Implementers must check `carrier.is_replay` before calling `install`. Failing
  to do so silently parents replay spans to expired traces — the test suite for
  the implementation ticket should assert this guard.
- Span links are best-effort: some APM tools render span links poorly. Operators
  lose the APM-level navigation from replay spans to original execution in such
  tools, falling back to manual trace-ID lookup.

---

## References

- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [OTel Rust SDK](https://docs.rs/opentelemetry)
- [Temporal trace propagation (Headers + interceptors)](https://docs.temporal.io/concepts/what-is-a-workflow#tracing-and-context-propagation)
- [DBOS OTel-native workflows](https://docs.dbos.dev/concepts/workflows)
- `autumn-harvest/src/telemetry.rs` — `ATTR_*` and `METRIC_*` constants (the
  code-level source of truth for this ADR)
