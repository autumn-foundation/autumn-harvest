# Durable Workflow Semantic Conventions

> **Draft for `open-telemetry/community`.** This page follows
> `project-template.md` at community commit `9c30b48`. Issue #2015 tracks
> it. Section 10 of the [conventions draft](README.md#10-submission) says
> when to file it. Delete this note before you file.

## Background and description

Durable workflow engines run code that survives a crash. Examples are
Temporal, Dapr Workflow, Restate, DBOS and Autumn Harvest. Each engine
records the steps of a run in a history. After a crash, the engine replays
the history to rebuild the run.

OpenTelemetry has no conventions for these engines. The attribute registry
has no `workflow` namespace. A [draft proposal](README.md) and its Weaver
model exist. The model passes the upstream policy checks.

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
- Each group starts at the `development` stability level.
- The model passes the upstream Weaver policy checks.

Why now: Dapr contributors began workflow conventions in 2026, in
`dapr/dapr#9213`. Engines that start now can share one model before each
one ships its own.

Benefits for OpenTelemetry:

- One vocabulary for a growing class of systems.
- Dashboards and alerts that work across engines.
- A replay rule that other areas with recorded history can reuse.

## Deliverables

1. An issue in `open-telemetry/semantic-conventions` that finds co-owners.
2. A `workflow` area: a Weaver model in `model/workflow/` and a page in
   `docs/workflow/`. The draft holds the model now.
3. Prototypes in two engines, in two different languages. Autumn Harvest
   gives a Rust prototype. A second engine gives one in another language,
   such as Go or Java.
4. A decision on each open question in section 8 of the draft.

Autumn Harvest emits its own `harvest.*` names today. Section 7 of the
draft lists [where Harvest does not conform](README.md#7-mapping-from-harvest).
Its prototype must close those gaps.

## Staffing / Help Wanted

The project needs people who build or instrument durable workflow engines.

### Industry outreach (Optional)

- Dapr: the authors of `dapr/dapr#9213`. Not contacted yet.
- Temporal, Restate and DBOS: their maintainers. Not contacted yet.

The upstream issue is the first contact.

### SIG

Open. The project asks the semantic convention maintainers to host it. It
proposes a new `Semantic Conventions: Workflow` area with its own
approvers.

### Required staffing

#### Project Leads(s)

Open. The Autumn Harvest maintainers offer one lead. The project needs a
second lead from another engine.

#### Other Staffing

Open. The project needs these people:

- One engineer for each prototype.
- Approvers from the semantic conventions SIG to review the prototypes.

### Sponsorship

#### TC Sponsor

Open. A semantic convention maintainer can act as sponsor. A TC
escalation sponsor then supports the project.

#### GC Liaison

Open.

## Expected Timeline

The timeline starts at the approval:

- Month 1: the leads decide the open questions of the draft.
- Month 2: a pull request adds the model and the page at `development`.
- Months 2 to 4: two engines build their prototypes.
- Month 5: the project reports the results and asks for feedback.

## Labels (Optional)

`area:workflow` for the issues and pull requests of the project.

## GitHub Project (Post-Approval)

After approval.

## SIG Meetings, Roadmap, and Other Info (Post-Approval)

After approval.
