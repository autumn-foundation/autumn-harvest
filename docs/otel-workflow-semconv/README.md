# Workflow semantic conventions — a draft proposal for OpenTelemetry

**Submission status:** pending. A maintainer files the text of section 9 in
`open-telemetry/semantic-conventions`. Then this line gets the issue link,
and `pending` changes to `submitted`.

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

The proposal adds a `workflow.*` namespace with nine attributes and eight
spans. One span describes each operation of a durable workflow engine. A
replay rule stops duplicate spans when an engine rebuilds a run from
history. Every group has the `development` stability level.

## 2. Motivation

Durable workflow engines run code that survives a crash. Examples are
Temporal, Dapr Workflow, Restate, DBOS and Harvest. Each one invents its
own span names and attributes. A dashboard for one engine does not work for
another.

OpenTelemetry has no workflow conventions today. The attribute registry has
no `workflow` namespace. Dapr began to codify its own telemetry with Weaver
in 2026, in `dapr/dapr#9213`.

Durable engines share a hazard that other areas do not have: replay. A
replay re-runs workflow code against recorded history. Naive instrumentation
then emits a second span for work that already happened. Section 6 gives a
rule against that.

## 3. Scope and prior art

In scope:

- Spans for the eight core operations. Three are for workflows: schedule,
  execute and start a child. Two are for activities: schedule and execute.
  Two are for signals: send and deliver. One fires a timer.
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
- Dapr proposes `PRODUCER` spans to schedule and `CONSUMER` spans to
  execute. Harvest uses `INTERNAL` spans to execute.

## 4. Attributes

| Attribute | Type | Brief |
|-----------|------|-------|
| `workflow.name` | string | The name of the workflow definition. Low-cardinality. |
| `workflow.run.id` | string | The unique identifier of one workflow run. High-cardinality. |
| `workflow.operation.name` | enum | The workflow operation that the span describes. |
| `workflow.queue.name` | string | The name of the task queue that holds the work item. |
| `workflow.activity.name` | string | The name of the activity definition. Low-cardinality. |
| `workflow.activity.attempt` | int | The attempt number of the activity call, starting at 1. |
| `workflow.replay` | boolean | Whether the workflow code runs as a replay of recorded history. |
| `workflow.signal.name` | string | The name of the signal. |
| `workflow.signal.id` | string | The unique identifier of one signal. High-cardinality. |

The spans also use the general `error.type` attribute on failure.

A high-cardinality attribute is for spans only. It must not become a metric
attribute.

## 5. Spans

| Span group | Kind | Span name | Required attributes |
|------------|------|-----------|---------------------|
| `span.workflow.schedule_workflow.producer` | `PRODUCER` | `schedule_workflow {workflow.name}` | operation, name, run id |
| `span.workflow.execute_workflow.internal` | `INTERNAL` | `execute_workflow {workflow.name}` | operation, name, run id, replay |
| `span.workflow.start_child_workflow.producer` | `PRODUCER` | `start_child_workflow {workflow.name}` | operation, child name, child run id |
| `span.workflow.schedule_activity.producer` | `PRODUCER` | `schedule_activity {workflow.activity.name}` | operation, activity name, run id |
| `span.workflow.execute_activity.internal` | `INTERNAL` | `execute_activity {workflow.activity.name}` | operation, activity name, run id, attempt |
| `span.workflow.send_signal.producer` | `PRODUCER` | `send_signal {workflow.name}` | operation, target run id |
| `span.workflow.deliver_signal.consumer` | `CONSUMER` | `deliver_signal {workflow.name}` | operation, name, run id |
| `span.workflow.fire_timer.internal` | `INTERNAL` | `fire_timer` | operation, run id |

The parent rules follow the kinds:

- A `PRODUCER` span is a child of the active span of the caller. The engine
  stores its trace context with the scheduled work.
- A worker restores that stored context. It then uses it as the parent of
  the `execute_workflow` or `execute_activity` span.
- `fire_timer` and the activity and child schedule spans are children of
  the `execute_workflow` span that emits them.

## 6. Replay

The key words MUST, MUST NOT, SHOULD and SHOULD NOT are as in RFC 2119.

1. An `execute_workflow` span MUST set `workflow.replay`. The value is
   `true` for a replay cycle and `false` for a live cycle.
2. Instrumentation MUST NOT emit an `execute_activity` span for an activity
   result that history already records. A replay reads the result. It does
   not run the activity again.
3. A replay cycle SHOULD start a new root span. It SHOULD NOT use the
   original trace context as its parent, because that trace can expire
   before the replay.
4. A replay cycle SHOULD add a span link to the original trace context when
   the engine still holds that context.
5. Instrumentation SHOULD NOT write trace context into the durable history.
   History outlives traces, so a stored context points at a trace that is
   gone.

Rule 2 is the core of the proposal. Without it, each replay adds a false
copy of every activity to the trace.

## 7. Mapping from Harvest

Spans, from ADR 0001 §2:

| Harvest name | Proposed name | Note |
|--------------|---------------|------|
| `harvest.workflow.schedule` | `span.workflow.schedule_workflow.producer` | |
| `harvest.workflow.execute` | `span.workflow.execute_workflow.internal` | |
| `harvest.child_workflow.start` | `span.workflow.start_child_workflow.producer` | |
| `harvest.activity.schedule` | `span.workflow.schedule_activity.producer` | |
| `harvest.activity.execute` | `span.workflow.execute_activity.internal` | |
| `harvest.signal.send` | `span.workflow.send_signal.producer` | |
| `harvest.signal.deliver` | `span.workflow.deliver_signal.consumer` | |
| `harvest.timer.fire` | `span.workflow.fire_timer.internal` | |
| `harvest.timer.start` | Not proposed. | Harvest emits it, but ADR 0001 does not list it. See section 8. |

Attributes, from the `ATTR_*` constants in `telemetry.rs`:

| Harvest name | Proposed name | Note |
|--------------|---------------|------|
| `harvest.workflow.id` | `workflow.name` | Harvest uses the word "id" for the workflow name. |
| `harvest.execution.id` | `workflow.run.id` | |
| `harvest.queue` | `workflow.queue.name` | |
| `harvest.activity.name` | `workflow.activity.name` | |
| `harvest.attempt` | `workflow.activity.attempt` | |
| `harvest.replay` | `workflow.replay` | |
| `harvest.signal.name` | `workflow.signal.name` | |
| `harvest.signal.id` | `workflow.signal.id` | |
| `harvest.target.execution.id` | `workflow.run.id` | On `send_signal`, the run attributes describe the target run. |
| `harvest.shard.id` | Not proposed. | Shards are a Harvest storage detail. Other engines partition in other ways. |

## 8. Open questions

1. **Execute span kind.** Harvest uses `INTERNAL`. Dapr proposes
   `CONSUMER`. A `CONSUMER` kind fits a worker that pulls from a queue.
2. **Name or type.** Temporal says "workflow type". Dapr and Harvest say
   "workflow name". This draft uses `workflow.name`.
3. **Run identity.** Temporal has a business workflow id and a run id.
   This draft has a run id only. A `workflow.id` attribute can follow.
4. **Queue.** `workflow.queue.name` can instead reuse
   `messaging.destination.name`. Issue #1838 does that for two metrics.
5. **Timer start.** Harvest also emits a span when it places a timer. The
   draft omits it until another engine needs it.
6. **Agents.** The GenAI conventions model agents and tools. A durable
   agent loop runs as a workflow, so the two areas must not overlap.

## 9. Upstream issue text

A maintainer files this text as a new issue in
`open-telemetry/semantic-conventions`. Check that each link is public
first.

```markdown
### Proposal: semantic conventions for durable workflow engines

#### Problem

Durable workflow engines (Temporal, Dapr Workflow, Restate, DBOS, Autumn
Harvest) each define their own span names and attributes. There is no
`workflow` namespace in the registry. Dashboards and alerts do not carry
across engines.

Durable engines also replay: they re-run workflow code against recorded
history. Without a shared rule, instrumentation emits duplicate spans for
work that already happened.

#### Proposal

A new `workflow` area at `development` stability:

- Nine attributes: `workflow.name`, `workflow.run.id`,
  `workflow.operation.name`, `workflow.queue.name`,
  `workflow.activity.name`, `workflow.activity.attempt`,
  `workflow.replay`, `workflow.signal.name`, `workflow.signal.id`.
- Eight spans, one for each operation: `schedule_workflow`,
  `execute_workflow`, `start_child_workflow`, `schedule_activity`,
  `execute_activity`, `send_signal`, `deliver_signal`, `fire_timer`.
- A replay rule: a replay cycle sets `workflow.replay`, starts a new root
  span with a link to the original trace, and emits no span for an
  activity result that history already records.

A draft Weaver model (`model/workflow/registry.yaml` and
`model/workflow/spans.yaml`) and a prototype exist:
https://github.com/autumn-foundation/autumn-harvest/tree/trunk-dev/docs/otel-workflow-semconv

#### Open questions

- `INTERNAL` or `CONSUMER` for execute spans.
- `workflow.name` or `workflow.type`.
- A separate business id beside `workflow.run.id`.
- Reuse of `messaging.destination.name` for the task queue.

#### Group

We look for co-owners from other engines. The Dapr maintainers began
similar work in dapr/dapr#9213.
```

## 10. Submission

The steps to submit:

1. Review this draft and the model.
2. Ask the Dapr maintainers to co-propose, through `dapr/dapr#9213`.
   Upstream asks for a group of owners for a new area.
3. File the section 9 text in `open-telemetry/semantic-conventions`.
4. Put the issue link on the status line. Change `pending` to `submitted`.

If the project decides not to submit, change `pending` to `declined` and
give the reason on the same line.

The guard suite checks the model shape and the names. It does not run
Weaver or the upstream policy checks. Upstream runs `make check` on the
pull request that follows the issue.
