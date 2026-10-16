# Workflow semantic conventions — a draft proposal for OpenTelemetry

**Submission status:** pending. A maintainer files the text of section 9 in
`open-telemetry/semantic-conventions`. Then this paragraph gets the issue
link, and `pending` changes to `submitted`.

This page answers issue #2015. It is a draft of OpenTelemetry semantic
conventions for durable workflow engines. It generalizes the span contract
of [ADR 0001](../adr/0001-otel-trace-contract.md) away from Harvest names.
The model files use the upstream Weaver format:

- [`model/workflow/registry.yaml`](model/workflow/registry.yaml) defines
  the attributes.
- [`model/workflow/spans.yaml`](model/workflow/spans.yaml) defines the
  spans.

The `otel_workflow_semconv_docs` guard suite keeps this page, the model and
ADR 0001 in agreement.

## 1. Summary

The proposal adds a `workflow.*` namespace. One span describes each core
operation of a durable workflow engine. A replay rule stops duplicate spans
when an engine rebuilds a run from history. Every group has the
`development` stability level.

## 2. Motivation

Durable workflow engines run code that survives a crash. Examples are
Temporal, Dapr Workflow, Restate, DBOS and Harvest. Each one invents its
own span names and attributes. A dashboard for one engine does not work for
another.

In October 2026, OpenTelemetry has no workflow conventions. The attribute
registry has no `workflow` namespace. Dapr began to codify its own
telemetry with Weaver in 2026, in `dapr/dapr#9213`.

Durable engines share a hazard that other areas do not have: replay. A
replay re-runs workflow code against recorded history. Naive instrumentation
then emits a second span for work that already happened. Section 6 gives a
rule against that.

## 3. Scope and prior art

In scope:

- Spans for the core operations. Workflows have schedule, execute and start
  a child. Activities have schedule and execute. Signals have send and
  deliver. Timers have fire.
- The attributes that those spans carry.
- The span rules for replay.

Out of scope:

- Metrics. Issue #1838 maps two Harvest metrics onto the messaging
  conventions instead.
- Logs and events.
- Updates, queries, continue-as-new and cancellation. A later revision can
  add them.

Prior art:

- `cicd.pipeline.*` models CI/CD pipeline runs and their tasks. A pipeline
  run is not durable and does not replay.
- `messaging.*` models brokers. A workflow task queue is often engine
  internal, so section 8 leaves the overlap open.
- The OTel trace API defines `CONSUMER` as the processing of an operation
  that a producer starts and does not wait for. A worker that runs
  scheduled work fits that kind, so the execute spans are `CONSUMER`.

## 4. Attributes

| Attribute | Type | Brief |
|-----------|------|-------|
| `workflow.system.name` | string | The name of the durable workflow engine. |
| `workflow.name` | string | The name of the workflow definition. Low-cardinality. |
| `workflow.run.id` | string | The unique identifier of one workflow run. High-cardinality. |
| `workflow.operation.name` | enum | The workflow operation that the span describes. |
| `workflow.queue.name` | string | The name of the task queue that holds the work item. |
| `workflow.activity.name` | string | The name of the activity definition. Low-cardinality. |
| `workflow.activity.attempt` | int | The attempt number of the activity call, starting at 1. |
| `workflow.replay` | boolean | Whether the cycle rebuilds workflow state from recorded history. |
| `workflow.signal.name` | string | The name of the signal. |
| `workflow.signal.id` | string | The unique identifier of one signal. High-cardinality. |

Each span carries the general `error.type` attribute when its operation
ends in an error.

A high-cardinality attribute is for spans only. It must not become a metric
attribute.

## 5. Spans

| Span group | Kind | Span name | Required attributes |
|------------|------|-----------|---------------------|
| `span.workflow.schedule_workflow.producer` | `PRODUCER` | `schedule_workflow {workflow.name}` | system, operation, name |
| `span.workflow.execute_workflow.consumer` | `CONSUMER` | `execute_workflow {workflow.name}` | system, operation, name, run id, replay |
| `span.workflow.start_child_workflow.producer` | `PRODUCER` | `start_child_workflow {workflow.name}` | system, operation, child name |
| `span.workflow.schedule_activity.producer` | `PRODUCER` | `schedule_activity {workflow.activity.name}` | system, operation, activity name, run id |
| `span.workflow.execute_activity.consumer` | `CONSUMER` | `execute_activity {workflow.activity.name}` | system, operation, activity name, run id, attempt |
| `span.workflow.send_signal.producer` | `PRODUCER` | `send_signal {workflow.name}` | system, operation, signal name |
| `span.workflow.deliver_signal.consumer` | `CONSUMER` | `deliver_signal {workflow.name}` | system, operation, name, run id, signal name |
| `span.workflow.fire_timer.internal` | `INTERNAL` | `fire_timer` | system, operation, run id |

Each span note in the model gives the start and end of its operation. If
the operation ends in an error, the span status SHOULD be `Error`.

Each span sets `workflow.operation.name` to its own operation. For example,
a `schedule_workflow` span MUST NOT carry `execute_activity`.

A run id is conditionally required where an engine can lack it. The span
MUST carry the run id when the id is available before the span ends. A
start that the server refuses has no run id. A signal to a workflow id can
have no run id.

The parent rules follow the kinds:

- A `PRODUCER` span is a child of the active span of the caller. The engine
  stores its trace context with the scheduled work.
- A worker restores that stored context. It then uses it as the parent of
  the `execute_workflow` or `execute_activity` span.
- `fire_timer`, `deliver_signal` and the activity and child schedule spans
  SHOULD be children of the `execute_workflow` span that observes them.

## 6. Replay

The key words MUST, MUST NOT, SHOULD and SHOULD NOT are as in RFC 2119.

1. An `execute_workflow` span MUST set `workflow.replay`. The value is
   `true` for a cycle that rebuilds state from history. It is `false` for
   a first cycle and for a resume from state in memory.
2. Instrumentation MUST NOT emit a span for an operation that a replay
   cycle reads from history. This covers `execute_activity` for a recorded
   result. It also covers `schedule_activity` and `start_child_workflow`
   for a recorded command. The first live application of an operation
   still gets its span, even when the engine writes the event first.
3. A replay cycle SHOULD start a new root span. It SHOULD NOT use the
   original trace context as its parent, because the original trace can
   be closed or dropped before the replay.
4. A replay cycle SHOULD add a span link to the original trace context when
   the engine still holds that context.
5. Instrumentation SHOULD NOT write trace context into the durable history.
   History outlives traces, so a stored context can point at a trace that
   the backend already dropped.

Rule 2 is the core of the proposal. Without it, each replay adds a false
copy of every recorded operation to the trace. Harvest emits its schedule
spans only for new commands, so it follows rule 2.

## 7. Mapping from Harvest

Spans, from ADR 0001 §2:

| Harvest name | Proposed name | Note |
|--------------|---------------|------|
| `harvest.workflow.schedule` | `span.workflow.schedule_workflow.producer` | |
| `harvest.workflow.execute` | `span.workflow.execute_workflow.consumer` | Harvest uses `INTERNAL`. |
| `harvest.child_workflow.start` | `span.workflow.start_child_workflow.producer` | |
| `harvest.activity.schedule` | `span.workflow.schedule_activity.producer` | |
| `harvest.activity.execute` | `span.workflow.execute_activity.consumer` | Harvest uses `INTERNAL`. |
| `harvest.signal.send` | `span.workflow.send_signal.producer` | |
| `harvest.signal.deliver` | `span.workflow.deliver_signal.consumer` | Harvest emits it as a root span today. |
| `harvest.timer.fire` | `span.workflow.fire_timer.internal` | Harvest emits it as a root span today. |
| `harvest.timer.start` | Not proposed. | Harvest emits it, but ADR 0001 does not list it. See section 8. |

Attributes, from the `ATTR_*` constants in `telemetry.rs`:

| Harvest name | Proposed name | Note |
|--------------|---------------|------|
| `harvest.workflow.id` | `workflow.name` | Harvest uses the word "id" for the workflow name. |
| `harvest.execution.id` | `workflow.run.id` | On `harvest.signal.send`, it is the target run. |
| `harvest.queue` | `workflow.queue.name` | |
| `harvest.activity.name` | `workflow.activity.name` | |
| `harvest.attempt` | `workflow.activity.attempt` | |
| `harvest.replay` | `workflow.replay` | Harvest uses the same definition. |
| `harvest.signal.name` | `workflow.signal.name` | Defined, not emitted. The signal spans emit the bare key `signal.name`. |
| `harvest.signal.id` | `workflow.signal.id` | Defined, not emitted. |
| `harvest.target.execution.id` | `workflow.run.id` | Defined, not emitted. On `send_signal`, the run attributes describe the target run. |
| `harvest.shard.id` | Not proposed. | Shards are a Harvest storage detail. Other engines partition in other ways. |

Harvest does not yet conform to every rule of this draft:

- It sets no `workflow.system.name` and no `workflow.operation.name`. The
  span name carries the operation.
- The strict-mode execute spans omit `harvest.workflow.id`.
- No execute span sets `error.type`.
- A replay cycle records the original `traceparent` as a `link.traceparent`
  attribute. It does not add a real span link.
- `harvest.timer.fire` and `harvest.signal.deliver` have no parent. ADR 0001
  §2.6 and §2.7 describe parents that the code does not set.

## 8. Open questions

1. **Execute span kind.** This draft uses `CONSUMER`, as the trace API
   defines it. Harvest uses `INTERNAL` today.
2. **Name or type.** Temporal says "workflow type". Dapr and Harvest say
   "workflow name". This draft uses `workflow.name`.
3. **Run identity.** Temporal has a business workflow id and a run id.
   This draft has a run id only. A `workflow.id` attribute can follow.
4. **Queue.** `workflow.queue.name` can instead reuse
   `messaging.destination.name`. Issue #1838 does that for two metrics.
5. **Timer start.** Harvest also emits a span when it places a timer. The
   draft omits it until another engine needs it.
6. **Agents.** The GenAI conventions model agents and tools. A durable
   agent loop can run as a workflow. The two areas need a clear boundary.
7. **Namespace.** "Workflow" also names CI/CD workflows and batch DAG
   tools. The area can need a narrower name, such as `durable_workflow`.
8. **System values.** `workflow.system.name` has no list of well-known
   values yet.
9. **Timer fire.** A timer fire is close to a point in time. The OTel
   guidance models such an occurrence as an event, not a span. This draft
   keeps the ADR 0001 span. An event on `execute_workflow` is the
   alternative.

## 9. Upstream issue text

A maintainer files this text as a new issue in
`open-telemetry/semantic-conventions`. Check that each link is public
first. Replace `<commit>` with the commit that holds the reviewed draft.

```markdown
### Proposal: semantic conventions for durable workflow engines

#### Problem

Durable workflow engines (Temporal, Dapr Workflow, Restate, DBOS, Autumn
Harvest) each define their own span names and attributes. The registry has
no `workflow` namespace. Dashboards and alerts do not carry across engines.

Durable engines also replay. A replay re-runs workflow code against
recorded history. Without a shared rule, instrumentation emits duplicate
spans for work that already happened.

#### Proposal

A new `workflow` area at `development` stability:

- Attributes: `workflow.system.name`, `workflow.name`, `workflow.run.id`,
  `workflow.operation.name`, `workflow.queue.name`,
  `workflow.activity.name`, `workflow.activity.attempt`,
  `workflow.replay`, `workflow.signal.name`, `workflow.signal.id`.
- One span for each operation: `schedule_workflow`, `execute_workflow`,
  `start_child_workflow`, `schedule_activity`, `execute_activity`,
  `send_signal`, `deliver_signal`, `fire_timer`.
- A replay rule. A replay cycle sets `workflow.replay`. It starts a new
  root span with a link to the original trace. It emits no span for an
  operation that a replay cycle reads from history.

A draft Weaver model (`model/workflow/registry.yaml` and
`model/workflow/spans.yaml`) is at
https://github.com/autumn-foundation/autumn-harvest/tree/<commit>/docs/otel-workflow-semconv

Autumn Harvest emits these spans under its own `harvest.*` names. Its
draft lists where it does not yet conform.

#### Open questions

- `CONSUMER` or `INTERNAL` for execute spans.
- `workflow.name` or `workflow.type`.
- A separate business id beside `workflow.run.id`.
- Reuse of `messaging.destination.name` for the task queue.
- A narrower namespace than `workflow`.
- A span or an event for `fire_timer`.

#### Group

The proposal needs co-owners from other engines. The Dapr maintainers
began similar work in dapr/dapr#9213.
```

## 10. Submission

The steps to submit:

1. Review this draft and the model.
2. Confirm the Dapr facts in `dapr/dapr#9213`. Ask the Dapr maintainers to
   co-propose. Upstream asks for a group of owners for a new area.
3. Read the project process in the `open-telemetry/community` repository.
   A new area can need a project and a sponsor.
4. Replace `<commit>` in the section 9 link with a merged commit or a tag.
   Then file the text in `open-telemetry/semantic-conventions`.
5. Put the issue link in the status paragraph. Change `pending` to
   `submitted`.

If the project decides not to submit, change `pending` to `declined` and
give the reason in the same paragraph.

The guard suite checks the model shape and the names. It does not run
Weaver or the upstream policy checks. Upstream runs `make check` on the
pull request that follows the issue.
