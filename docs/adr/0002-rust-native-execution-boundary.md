# ADR 0002 - Rust-Native Execution Boundary

**Status**: Accepted  
**Date**: 2026-05-03  
**Issue**: None

---

## Context

Autumn Harvest is a durable workflow engine written in Rust. Its core value is
that workflow execution, activity dispatch, replay, retries, cancellation,
heartbeats, history persistence, and observability share one strongly typed
runtime model.

Polyglot activity workers were considered as a future capability. The obvious
shape would be a gRPC/Protobuf worker protocol where non-Rust processes poll
for activity leases, heartbeat, and report terminal results. That architecture
is viable, but it creates a large permanent surface area:

- A versioned wire protocol and compatibility policy.
- Official SDKs for each supported language.
- Cross-language payload encoding rules.
- Conformance tests for retries, heartbeats, cancellation, stale attempts, and
  duplicate completions in every SDK.
- Additional deployment, debugging, and support modes.
- Weaker compile-time guarantees at the activity boundary.

For Autumn's current product direction, this does not unlock enough value to
justify the maintenance burden. Most non-Rust integration needs can already be
served by explicit external boundaries: external activities, task tokens,
signals, webhooks, management APIs, and durable callback completion.

The architectural question is whether Autumn should pursue first-class polyglot
activity execution, or deliberately remain Rust-native and treat non-Rust code
as an external system.

---

## Decision

Autumn Harvest is a **Rust-native durable workflow engine**.

First-class workflow and activity authoring is Rust-only. The engine will not
maintain official polyglot worker runtimes, language SDKs, or a remote activity
worker protocol unless this ADR is superseded by a future decision.

Non-Rust systems integrate through explicit external boundaries:

- External activities with durable task tokens.
- Signals into running workflows.
- Webhooks or HTTP APIs that complete external tasks.
- Management API calls for operational workflows.
- Application-owned adapters that translate between Autumn and non-Rust
  systems.

Autumn remains responsible for all durable workflow semantics:

- Event history writes.
- Activity scheduling.
- Attempt tracking.
- Retry decisions.
- Timeout enforcement.
- Cancellation state.
- Heartbeat and liveness interpretation.
- Workflow wakeups after external completion.

External systems may perform work, but they do not own workflow state and do not
write history directly.

---

## Architecture Boundary

The intended boundary is:

```text
Rust workflow code
  -> Rust activity code
  -> Autumn task queue
  -> Autumn event history
```

When another runtime is required, it crosses an explicit integration boundary:

```text
Rust workflow code
  -> external activity token
  -> non-Rust system
  -> callback/webhook/API completion
  -> Autumn event history
```

This keeps the deterministic runtime small and auditable. Rust activities remain
the ergonomic default. External activity completion remains the escape hatch for
systems that genuinely need another runtime, a human approval step, a SaaS
callback, or a separately deployed service.

---

## Non-Goals

Autumn will not, under this decision:

- Provide official Python, Node, Go, Java, C#, or JVM worker SDKs.
- Add a gRPC/Protobuf remote worker protocol as a first-class runtime path.
- Support non-Rust workflow definitions.
- Let external processes claim internal task queue rows directly.
- Let external systems append workflow history events directly.
- Treat polyglot execution as a required competitive checkbox.

Teams may still build application-specific adapters on top of the public API,
but those adapters are not part of Autumn's core execution contract.

---

## Deferred Alternative

If real user demand appears later, the preferred future design is trusted remote
activity workers over gRPC/Protobuf:

```text
Autumn gRPC gateway
  -> RegisterWorker
  -> PollActivity
  -> HeartbeatActivity
  -> CompleteActivity
  -> FailActivity
```

That design remains intentionally deferred. If adopted, it must be introduced
behind a new ADR with a formal protocol contract, conformance suite, and clear
support policy.

---

## Consequences

**Positive**:

- The core engine can focus on doing one thing extremely well: Rust-native
  durable workflows.
- Workflow and activity registration keep strong Rust typing and macro support.
- Replay, cancellation, retries, sagas, updates, heartbeats, and telemetry stay
  inside one implementation model.
- The project avoids maintaining a long tail of language SDKs and subtly
  different runtime behaviors.
- External activity tokens still cover important integration cases without
  expanding the core execution model.

**Negative / trade-offs**:

- Some teams will prefer engines with first-class polyglot worker SDKs.
- Non-Rust activities require an explicit external integration step instead of
  direct worker registration.
- Language-native SDKs for Python ML, Node SaaS clients, or JVM enterprise
  libraries are not first-class Autumn features.
- Adoption may be narrower, but the product identity is sharper.

---

## Operating Principle

Autumn does not chase polyglot execution. It provides Rust-native durable
workflows, and integrates with other runtimes through explicit external
boundaries.

Proofs prevent forbidden states; tests prevent forgotten behavior. Polyglot
worker protocols are neither proof nor test; they are a maintenance mortgage.

---

## References

- `autumn-harvest/src/context.rs` - `ScheduleExternalActivity` command and
  workflow/activity execution boundary.
- `autumn-harvest/src/external_task.rs` - durable task-token completion.
- `autumn-harvest/src/worker.rs` - worker runtime, task claiming, dispatch,
  timeout, retry, and wakeup semantics.
- `README.md` - external completion, worker fleet, and architecture summary.
