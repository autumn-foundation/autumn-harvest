# Review: Vantage Spec for Saga Primitives

**Spec:** `docs/plans/vantage-spec-saga-primitives.md`
**Date:** 2026-04-24
**Target phase:** Phase 4

## TL;DR

The spec captures the right user-facing outcome — durable, automatic
compensation of multi-step workflows — but does not acknowledge that
`autumn-harvest/src/saga.rs` already exists, and leaves enough design
questions open that two implementers would ship incompatible PRs. Concept
approved; spec not yet implementable as written.

## 1. The spec ignores prior art in this repo

`autumn-harvest/src/saga.rs` already ships a `Saga<'ctx>` builder with
`step(forward, compensate)`, `compensate_all()`, and a
`HarvestError::SagaCompensationFailed` variant combining the original error
with compensation failures (`src/saga.rs:23-136`). Five unit tests plus
`tests/saga_tests.rs` cover LIFO rollback, manual triggering, and
compensation-of-compensation failures.

This helper stores compensations as `Vec<Box<dyn FnOnce() -> BoxFuture<...>>>`
on the `Saga` struct (`src/saga.rs:14, 25`) and runs them inline. On worker
crash the `Vec` is gone, so the chain cannot be resumed. That directly fails
the spec's own acceptance criterion *"compensation actions are executed
durably … surviving process restarts and worker crashes."*

The spec must pick one: **replace** the helper, **extend** its surface onto a
durable backend, or **coexist** (keep the in-memory helper, add a separate
`DurableSaga`). This is the single biggest blocker.

## 2. Blocking ambiguities

1. **"Terminal failure" is undefined.** `executor.rs` classifies outcomes as
   `Completed`, `Failed`, `Suspended` (`src/executor.rs:25-35`). Which of
   these trigger the compensation chain? Handler-returned `Err` is obvious;
   externally cancelled, panicked, or DLQ-exhausted-forward-activity are not.
   Cross-references the Phase 4 cancellation spec and needs to be pinned
   down in one of the two documents.

2. **"Reverse order of completion" is ambiguous under concurrency.** Workflows
   can `futures::join!` activities. Spec should pick reverse-registration LIFO
   (matches the existing helper) or wall-clock completion order and say so.

3. **No API surface is chosen.** Macro attribute
   (`#[activity(compensation = "…")]`), explicit builder (`saga.step(fwd,
   comp, input)`), and context method (`ctx.execute_activity_with_compensation`)
   all satisfy the ACs. Without a pick, the implementer invents one.

4. **New `WorkflowEvent` variants are not listed.** Any durable implementation
   needs at least `CompensationRegistered / Scheduled / Completed / Failed`
   appended to the 17-variant enum in `src/event.rs:20`. Without a
   per-compensation `Completed` event, a worker that crashes mid-chain cannot
   determine which compensations already ran. Spec must reaffirm the
   append-only invariant (CLAUDE.md) and enumerate the additions.

5. **Workflow end-state after a poison-pilled compensation is unspecified.**
   Does the workflow stay `Running` awaiting operator intervention, or move
   to `Failed` immediately with a DLQ breadcrumb? `dlq.rs`'s `DeadLetterEntry`
   also lacks any link back to the forward activity being compensated;
   operators will not be able to triage without one.

## 3. Secondary concerns

- **Compensation idempotency.** Durable retries mean at-least-once delivery;
  the spec should require compensations to be idempotent and the engine to
  record outcomes at-most-once in the event log.
- **Input/output serialisation.** Today's helper passes `T: Clone` through a
  closure. A durable version needs forward output `Serialize` + compensation
  input `DeserializeOwned`. Breaking change for existing helper users.
- **Retry policy inheritance.** Should a compensation inherit the forward
  activity's `RetryPolicy` or declare its own? Inheritance is a footgun —
  "release inventory" and "charge card" want different policies. Default to
  the compensation activity's own attribute, no implicit inheritance.
- **Child workflows.** If a parent fails with a running child, does the child
  finish/cancel before the parent's chain runs? Not addressed.
- **Chain deadline.** No configurable upper bound on total chain runtime;
  long chains with aggressive retries can hang a workflow indefinitely.
- **Metrics.** Phase 4 also introduces observability. Chain latency,
  compensation failure rate, and DLQ poisoning rate are high-value metrics
  that should land in the OpenTelemetry spec alongside this one.

## 4. Out-of-scope list is correct

No 2PC, no inferred "reverse," no synchronous rollbacks — all match industry
norm. Worth making two implicit exclusions explicit: (a) the engine does not
auto-save forward outputs to any external store — the workflow passes them
into the compensation like any other activity input; (b) cross-workflow
compensation is not automatic — a parent must observe child failure and
trigger its own chain.

## 5. Verdict

Concept: **approved.** Spec as written: **not yet implementable** — a short
editing pass by the spec owner resolves §§1-2. Estimated implementation size
once unblocked: one non-trivial PR touching roughly event.rs, saga.rs,
context.rs, executor.rs, dlq.rs, plus one migration and an expanded
`tests/saga_tests.rs` with crash-and-replay coverage.
