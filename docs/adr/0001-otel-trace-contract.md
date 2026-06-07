# ADR 0001 — OpenTelemetry Trace Propagation Contract

**Status**: Accepted  
**Date**: 2026-05-01  
**Issue**: [#93](https://github.com/madmax983/autumn-harvest/issues/93)

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

The following metrics are defined by the constants in `telemetry.rs`. The
`MetricsRecorder` trait method that drives each metric is listed alongside it.

| Rust constant              | Metric name                   | Instrument   | Labels (cardinality)                          | Forbidden labels      |
|----------------------------|-------------------------------|--------------|-----------------------------------------------|-----------------------|
| `METRIC_WORKFLOW_STARTED`  | `harvest.workflow.started`    | Counter      | `workflow.name` (bounded), `queue` (bounded)  | `execution.id`        |
| `METRIC_WORKFLOW_DURATION` | `harvest.workflow.duration`   | Histogram    | `workflow.name`, `queue`, `status`            | `execution.id`        |
| `METRIC_WORKFLOW_TERMINAL` | `harvest.workflow.terminal`   | Counter      | `workflow` (= `METRIC_LABEL_WORKFLOW`), `queue`, `outcome` (6 values: completed/failed/cancelled/timed_out/terminated/continued_as_new) | `execution.id` |
| `METRIC_ACTIVITY_DURATION` | `harvest.activity.duration`   | Histogram    | `activity.name` (bounded), `queue`, `status` | `execution.id`        |
| `METRIC_TIMER_STARTED`     | `harvest.timer.started`       | Counter      | _(none)_                                      |                       |
| `METRIC_QUEUE_DEPTH`       | `harvest.queue.depth`         | Gauge        | `queue` (bounded)                             | `execution.id`        |
| `METRIC_DLQ_ENTRIES`       | `harvest.dlq.entries`         | Gauge        | `shard` (≤ 256)                               |                       |
| `METRIC_SCHEDULE_RUNS`     | `harvest.schedule.runs`       | Counter      | `kind` (2 values), `name` (bounded)           |                       |
| `METRIC_SCHEDULE_SKIPPED`  | `harvest.schedule.skipped`    | Counter      | `kind`, `name`, `reason` (3 values)           |                       |
| `METRIC_RETENTION_DELETED` | `harvest.retention.deleted`   | Counter      | `shard` (≤ 256)                               |                       |

**Cardinality rule**: `execution.id` (a UUID) is **explicitly forbidden** as a
metric label. It is unbounded and would explode the metric time-series in any
production APM. Use `ATTR_EXECUTION_ID` only on *spans*, never on *metrics*.

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
