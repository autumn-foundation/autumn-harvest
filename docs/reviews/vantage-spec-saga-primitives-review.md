# Review: Vantage Spec for Saga Primitives

**Spec under review:** `docs/plans/vantage-spec-saga-primitives.md`
**Reviewer:** Claude (automated spec review)
**Date:** 2026-04-24
**Target phase:** Phase 4

## TL;DR

The spec captures the right *user-facing* outcome — durable, automatic
compensation of multi-step workflows — but is silent on enough implementation
questions that two engineers reading it would ship incompatible designs. It
also does not acknowledge that `autumn-harvest/src/saga.rs` already exists as an
in-memory helper, which directly contradicts the spec's first two acceptance
criteria. Before implementation starts, the spec needs decisions on (1) the
relationship to the existing helper, (2) new `WorkflowEvent` variants, (3) the
public API surface (macro attribute vs. explicit `Saga` builder), and (4) what
exactly "terminal failure" means.

## 1. The spec ignores prior art in this repo

`autumn-harvest/src/saga.rs` already ships a `Saga<'ctx>` builder with
`step(forward, compensate)`, `compensate_all()`, and a
`HarvestError::SagaCompensationFailed` variant combining the original error
with a list of compensation failures (`src/saga.rs:23-136`). There are five
unit tests covering successful steps, LIFO compensation on step failure,
compensation-of-compensation failures, and the manual-trigger path
(`src/saga.rs:144-314`), plus an integration file at `tests/saga_tests.rs`.

This helper:

- holds compensations as `Box<dyn FnOnce() -> BoxFuture<...>>` closures in a
  `Vec` on the `Saga` struct (`src/saga.rs:14, 25`)
- runs compensations inline as ordinary async code

which means it **fails the spec's first two acceptance criteria on its own**:

- "register a compensation action (which is itself an activity)" — today it
  accepts any closure, not just activities, and nothing is registered in the
  event log.
- "compensation actions are executed durably … surviving process restarts and
  worker crashes" — on worker restart the `Vec<Compensation>` is gone, so the
  chain cannot be resumed.

The spec should explicitly state one of:

1. **Replace** the in-memory helper. Rename or delete `Saga<'ctx>`, migrate
   tests, introduce a new durable type.
2. **Extend** the in-memory helper. Keep the surface, change `step` to require
   activity references, thread registrations through the event log.
3. **Coexist.** Keep `Saga<'ctx>` for simple single-process orchestration; add
   a second `DurableSaga` for the spec's guarantees.

Option 2 minimises churn for any user that is already on the helper, but the
spec must commit to a path — otherwise the implementer picks, and reviewers
will disagree.

## 2. Acceptance criteria have load-bearing ambiguity

### "Terminal failure"

> Must allow automatic execution of the compensation chain … when a workflow
> encounters a terminal failure.

`executor.rs` currently classifies outcomes as `Completed`, `Failed`, or
`Suspended` (`src/executor.rs:25-35`). The spec must say which of these count
as "terminal":

- Workflow handler returned `Err(E)` — **yes, clearly**.
- Forward activity exhausted retries and was moved to DLQ, and the workflow
  never observed the error (e.g., it was awaiting elsewhere) — **unclear**.
- Workflow is externally cancelled (Phase 4 cancellation spec) — **unclear;
  cross-spec dependency**.
- Workflow panics — **unclear**.

Recommendation: explicitly enumerate the triggers, and cross-reference the
cancellation spec for the cancellation case (probably: *run compensations by
default, opt-out via a flag*).

### "Reverse order of completion"

Workflows can drive activities concurrently (`futures::join!`, `FuturesUnordered`).
"Reverse order of completion" is ambiguous when two activities complete in
parallel. Pick one and say so:

- **Wall-clock completion order** — needs a monotonic timestamp on
  `ActivityCompleted`; already present via event ordering.
- **Reverse registration order** — matches the current in-memory helper; does
  not respect parallel completion.

The existing helper does reverse-registration LIFO (`src/saga.rs:124`). Keeping
that is the simplest answer; the spec should just say so.

### "Compensation is itself an activity"

This is a useful constraint but implies:

- The compensation function must be registered in `activities![...]` at
  builder time. The activity macro (`autumn-harvest-macros/src/activity.rs`) is
  the natural registration surface.
- The compensation's input must be `Serialize` — it will be serialized into
  `harvest_task_queue.input` and replayed from history after a crash. The
  current in-memory helper passes `T: Clone` through a closure with no
  serialization requirement. This is a breaking change for any user of the
  existing helper.
- The forward activity's *output* must also be `Serialize`, because that
  output is typically the compensation's input. Today that is already required
  by the executor's history model, so no new constraint — but the spec should
  say it.

### "Durably, with their own configurable retry policies"

Good. Reuses `EnqueueParams.retry_policy` and the existing task-queue path.
The spec should clarify whether:

- the compensation's retry policy defaults to the forward activity's policy,
  **or**
- the developer must provide one explicitly on the compensation activity.

Recommendation: default to the compensation activity's own `#[activity(retry =
…)]` attribute, with no implicit inheritance. Inheritance is a footgun because
forward and rollback operations typically have very different retry
characteristics (e.g., "release inventory" should retry forever; "charge card"
should not).

### "Poison-pill compensations move to DLQ"

`dlq.rs` already has a `DeadLetterEntry` with `task_type`, `activity_name`,
`workflow_exec_id`, `input`, `error`, `attempts`. For a compensation DLQ entry
to be operationally useful, an operator needs to know:

- which forward activity this compensation was rolling back (needs a new
  `compensation_for_activity_exec_id` column, or a discriminator in a JSONB
  `metadata` column)
- whether the workflow is still waiting on this compensation, and what state
  the workflow is in once the compensation poisons out

The spec punts on both. Add explicit language:

- **Workflow state after DLQ:** the workflow is marked `Failed` with a
  dedicated error (e.g., `HarvestError::CompensationPoisoned { activity,
  compensation, ... }`). It does not stay `Running` waiting for the operator.
- **Operator retry from DLQ:** in scope? Out of scope? If in, this is a
  sub-feature worth its own section (requires re-enqueueing with attempt
  counter reset, replay-safe).

## 3. API surface is not specified

The spec says "developers can register a compensation action" but does not
pick a surface. Options:

| Surface | Example | Pros | Cons |
|---|---|---|---|
| Macro attribute | `#[activity(compensation = "release_inventory")]` | Compiler-enforced pairing; compensation registered once, used anywhere the forward activity is called | Inflexible: same compensation for all call sites, can't close over call-site context |
| Explicit builder | `saga.step(&reserve_inventory, &release_inventory, input)` | Per-call-site pairing; closes over call-site state naturally | Two things to keep in sync; easy to forget |
| Context method | `ctx.execute_activity_with_compensation(...)` | Discoverable; no new type | Long name; clutter on `WorkflowContext` |

All three can coexist, but the spec should pick a **primary** surface and list
the others as follow-ups. Without that decision the implementer will ship
whichever one feels easiest on the day.

My recommendation: **explicit builder** as primary (matches existing
`Saga<'ctx>` muscle memory), with the macro attribute deferred to a follow-up
spec because it forces a single compensation per activity definition.

## 4. Event-model impact is not called out

The spec never mentions `WorkflowEvent`, but any durable implementation forces
new variants. `event.rs` currently has 17 variants (`src/event.rs:23-145`); the
file-level invariant is **append-only, never reorder, never remove**
(CLAUDE.md, "Append-only event invariant").

At minimum the following variants need appending:

- `CompensationRegistered { for_activity_exec_id, compensation_activity, input }`
  — emitted when a saga step succeeds and the compensation is queued for
  potential later execution.
- `CompensationScheduled { compensation_exec_id }` — emitted when the
  compensation chain runs and this compensation is enqueued as a task.
- `CompensationCompleted { compensation_exec_id, result }` — needed so replay
  can skip compensations that already ran before a crash.
- `CompensationFailed { compensation_exec_id, error, poisoned: bool }` —
  `poisoned = true` differentiates DLQ-routing from a retryable failure.

Without `CompensationCompleted`, partial rollback across crash is not
recoverable: a worker that dies after 3-of-5 compensations cannot know which
have run. The spec should explicitly require per-compensation outcome events.

## 5. Schema impact

Existing tables (CLAUDE.md + `migrations/20260409000000_harvest_initial/`):

- `harvest_task_queue` can carry compensations by reusing `task_type =
  Activity` + an extra JSON metadata field.
- `harvest_dead_letters` does not distinguish compensation-DLQ entries; see §2.
- `harvest_events` is fine for the new variants (JSONB payload).

Minimum schema change: one additional optional column on
`harvest_dead_letters` (e.g., `compensates_activity_exec_id UUID NULL`). This
is small and backwards-compatible, but the spec should commit to it rather
than leaving it to the implementer.

## 6. Operational and correctness concerns the spec does not cover

1. **Compensation idempotency.** Durable retries mean a compensation may run
   more than once on the wire before the engine records
   `CompensationCompleted`. The spec should state: *compensations must be
   idempotent*, and the engine provides at-least-once delivery with
   at-most-once **recording** in the event log.
2. **Compensation-of-compensation failure.** What happens if
   `CompensationA` succeeds, `CompensationB` poison-pills, and the operator
   later triggers a retry from the DLQ? Does `CompensationA` re-run? (It
   shouldn't — the event log marks it completed.)
3. **Interaction with child workflows.** If a parent workflow fails and has a
   running child, does the child's saga chain run first? The spec is silent.
   Recommendation: children complete/cancel (per cancellation spec), then the
   parent's compensation chain runs.
4. **Time bound.** Is there a configurable deadline for "total time spent
   running the compensation chain"? Long chains with aggressive retries can
   hang a workflow in `Compensating` state indefinitely.
5. **Observability hook.** Phase 4 also introduces metrics/observability.
   Compensation-chain latency, compensation failure rate, and DLQ poisoning
   rate are high-value metrics. The spec should list them so they land in the
   OpenTelemetry spec too.

## 7. Out-of-scope list looks right

- No 2PC — correct.
- No inference of "reverse of an activity" — correct, matches industry norm.
- No synchronous rollbacks — correct, and consistent with the rest of the
  engine's execution model.

Two additions worth calling out explicitly:

- No automatic **saving** of forward activity outputs to an external store —
  the workflow must pass outputs into the compensation step like any other
  activity input.
- No cross-workflow compensation (a child workflow's failure does not
  automatically compensate steps in the parent; the parent must observe the
  child failure and trigger its own chain).

## 8. Suggested edits before implementation

In priority order:

1. **Add a "Relationship to existing `Saga<'ctx>` helper" section.** Pick
   replace / extend / coexist. This is blocking.
2. **Add an "Event model" section** listing the new `WorkflowEvent` variants
   and reaffirming the append-only invariant.
3. **Add an "API" section** picking one primary developer surface.
4. **Tighten "terminal failure"** into an explicit enumeration.
5. **Specify "reverse order"** as reverse-registration LIFO.
6. **Specify default retry policy for compensations** (own attribute, no
   inheritance).
7. **Specify workflow end-state when a compensation poison-pills** (Failed +
   new error variant; workflow does not block on operator).
8. **Add "Compensations must be idempotent"** to the out-of-scope /
   constraints section.
9. **Add a short "Interaction with cancellation"** note cross-referencing the
   cancellation spec.
10. **Add a short "Metrics"** note cross-referencing the OpenTelemetry spec.

## 9. Verdict

Concept and business value: **approved**. Scope and API: **not yet
implementable from this spec alone**. The gaps above are all design
decisions, not research questions — a 30-minute editing pass by the spec
owner resolves them. After that the implementation is roughly:

- append four event variants (event.rs)
- add one column to `harvest_dead_letters` (new migration)
- replace `Saga<'ctx>`'s internal storage with an event-log-backed
  registration path (saga.rs, context.rs)
- add a hook in `executor.rs` that, on terminal failure, enqueues the
  compensation chain as ordinary activity tasks
- extend `dlq.rs` to attach `compensates_activity_exec_id` metadata

Estimated size: one non-trivial PR touching ~8 files plus one migration plus
tests. The existing test scaffolding in `tests/saga_tests.rs` is a good base
to expand with crash-and-replay integration tests using testcontainers.
