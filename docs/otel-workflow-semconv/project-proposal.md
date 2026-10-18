# Durable Workflow Semantic Conventions

> **Draft for `open-telemetry/community`.** This page follows the community
> `project-template.md` at the commit that section 10 of the conventions
> draft names. Issue #2015 tracks it. Section 10 says when to file it.
> Delete this note before you file.

## Background and description

Durable workflow engines run code that survives a crash. Examples are
Temporal, Dapr Workflow, Restate, DBOS and Autumn Harvest. Each engine
records the steps of a run in a history. After a crash, the engine replays
the history to rebuild the run.

OpenTelemetry has no conventions for these engines. The attribute registry
has no `workflow` namespace. A
[conventions draft](https://github.com/autumn-foundation/autumn-harvest/tree/<commit>/docs/otel-workflow-semconv)
holds a Weaver model. The model passes the upstream policy checks. The
issue `<issue>` asks for co-owners from other engines.

`open-telemetry/semantic-conventions#1688` proposes general `workflow.*`
attributes for tasks, pipelines and jobs. It uses `workflow.name` and
`workflow.run.id`, and the conventions draft uses the same names. This
project adds what durable engines need: the spans of the core operations
and a rule for replay.

### Current challenges

- Each engine has its own span names and attributes. A dashboard or an
  alert for one engine does not work for another engine.
- A replay re-runs workflow code against history. Naive instrumentation
  then emits a second span for work that already happened. Users see false
  work in their traces.
- One run can last for days or months. Each engine handles the trace
  context of a resumed run in its own way.
- A backend cannot show a workflow run as one unit across engines.

If nothing is done, each engine ships its own conventions. Later
alignment then needs breaking changes in each engine.

### Goals, objectives, and requirements

Goals:

- Define span conventions for the core operations of a durable workflow
  engine.
- Define one rule for spans during replay.
- Prove the conventions in prototypes in two engines.

Requirements:

- The conventions use no name that is specific to one engine.
- The names agree with the general `workflow.*` attributes of
  `open-telemetry/semantic-conventions#1688`.
- Each group starts at the `development` stability level.
- The model passes the upstream Weaver policy checks.

Why now: in November 2025, Dapr contributors opened `dapr/dapr#9213`. It
adds a Weaver model with Dapr workflow conventions. It closed as stale in
April 2026 and did not merge. An
[OpenTelemetry blog post](https://opentelemetry.io/blog/2026/dapr-workflow-observability/)
says that Dapr is adopting Weaver. Engines that start now can share one
model before each one ships its own.

Benefits for OpenTelemetry:

- One vocabulary for a growing class of systems.
- Dashboards and alerts that work across engines.
- A replay rule that other areas with recorded history can reuse.

## Deliverables

1. Prototypes in two engines, in two different languages. Autumn Harvest
   gives a Rust prototype. A second engine gives one in another language,
   such as Go or Java.
2. A `workflow` area: a Weaver model in `model/workflow/` and a page in
   `docs/workflow/`. The conventions draft holds the model now.
3. A decision on each open question in section 8 of the conventions draft.

Autumn Harvest emits its own `harvest.*` names today. Section 7 of the
conventions draft lists
[where Harvest does not conform](https://github.com/autumn-foundation/autumn-harvest/blob/<commit>/docs/otel-workflow-semconv/README.md#7-mapping-from-harvest).
Its prototype must close those gaps.

## Staffing / Help Wanted

The project needs people who build or instrument durable workflow engines.

### Industry outreach (Optional)

- Dapr: the authors of `dapr/dapr#9213`. Not contacted yet.
- The authors of `open-telemetry/semantic-conventions#1688` and the CI/CD
  SIG. Not contacted yet.
- Temporal, Restate and DBOS: their maintainers. Not contacted yet.

The upstream issue is the first contact.

### SIG

A new SIG: Semantic Conventions: Workflow. Its approvers own the new
`workflow` area.

### Required staffing

#### Project Leads(s)

Open. The project needs leads from at least two engines. The leads become
the approvers of the new SIG.

#### Other Staffing

Open. The project needs these people:

- One engineer for each prototype.
- Maintainers or approvers of the language SIGs of the prototypes, to
  review them.

### Sponsorship

#### TC Sponsor

Open. The project needs a TC Escalation Sponsor.

#### Delegated TC Sponsor (Optional)

Open. A maintainer of the Semantic Conventions SIG can sponsor the project.

#### GC Liaison

Open.

## Expected Timeline

The timeline starts at the approval:

- Month 1: the leads decide the open questions of the conventions draft.
- Months 1 to 3: two engines build their prototypes.
- Month 3: a pull request adds the model and the page at `development`.
  It links the prototypes.
- Month 4: the project reports the results and asks for feedback.

## Labels (Optional)

`area:workflow` in `open-telemetry/semantic-conventions`. The `areas.yaml`
entry of the project creates it. Its name follows the decision on the
namespace.

## GitHub Project (Post-Approval)

After approval.

## SIG Meetings, Roadmap, and Other Info (Post-Approval)

After approval.
