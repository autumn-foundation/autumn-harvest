# Getting Started with Autumn Harvest

This is the long-form companion to [`examples/quickstart`](../../examples/quickstart). The
quickstart gets a single workflow running in five minutes; this guide walks
through the rest of the surface — activities, retries, durable timers, signals,
child workflows, idempotency, and the management API — by growing one example
into something that resembles a real service.

By the end you'll have:

- A running Autumn web app with the Harvest plugin mounted at `/api/harvest`.
- One workflow that orchestrates two activities, a 30-second timer, and a
  `payment_captured` signal handoff.
- A child workflow for invoice generation.
- Idempotent downstream calls via `ctx.idempotency_key()`.
- The dashboard, preflight, and `harvest` CLI wired up against your local
  service.

Stop at any chapter — each one ends in a runnable state.

> **Prerequisites**
> - Stable Rust toolchain (`rustup default stable`)
> - Docker (for Postgres via the example's `compose.yaml`)
> - `jq` (optional, used in the curl examples)

## Chapters

1. [Project skeleton](01-project-skeleton.md) — Cargo deps, plugin mount, dev profile migrations.
2. [Your first workflow and activity](02-first-workflow.md) — `#[workflow]`, `#[activity]`, the attribute reference.
3. [Durable timers](03-durable-timers.md) — `ctx.timer()` and the kill-and-restart durability demo.
4. [Signals](04-signals.md) — `wait_for_signal` for human / webhook / cross-system handoffs.
5. [Child workflows](05-child-workflows.md) — composing orchestrations with `spawn_child_workflow_raw`.
6. [Idempotency](06-idempotency.md) — `ctx.idempotency_key()` and subkeys for at-least-once safety.
7. [Reliability knobs](07-reliability-knobs.md) — retries, concurrency caps, local activities, queues, versioning, search attributes.
8. [DAGs and schedules](08-dags-and-schedules.md) — `#[dag]`, `DagBuilder`, trigger rules, cron schedules, manual triggers, offline lint/sim/profile.
9. [Worker routing and capabilities](09-worker-routing.md) — Queue name partitioning, Build-ID compatibility, and capability labels.
10. [Operating the service](10-operations.md) — preflight, dashboard, CLI, DLQ, worker drain, reuse policies.
11. [Testing your workflow code](11-testing.md) — unit tests and `WorkflowReplayer` regression coverage.
12. [Inbound webhooks](12-webhooks.md) — `#[webhook]`, `[security.webhooks]` verification, idempotent dispatch.

Start with [Chapter 1 →](01-project-skeleton.md)

## Where to go next

- **Reference example.** [`examples/billing-autumn-web/`](../../examples/billing-autumn-web/)
  is a full subscription-checkout integration: outbox → workflow start, saga
  compensation, child workflow, version gate, signal handoff, and a scheduled
  reconciliation DAG.
- **Standalone runner.** [`examples/standalone-runner/`](../../examples/standalone-runner/)
  shows the engine without `HarvestPlugin` — useful when embedding in a
  non-Autumn service.
- **Embedded SQLite backend.** [`sqlite-backend.md`](../sqlite-backend.md) is a
  task-oriented guide to `autumn-harvest-sqlite`, a single-writer, no-server
  persistence backend for edge / local-first / single-server deployments (its
  runnable examples live in
  [`autumn-harvest-sqlite/examples/`](../../autumn-harvest-sqlite/examples/)).
- **Runbooks.**
  [`audit-trail.md`](../runbooks/audit-trail.md),
  [`external-activity-handoffs.md`](../runbooks/external-activity-handoffs.md),
  [`replay-fixture-export.md`](../runbooks/replay-fixture-export.md),
  [`version-gate-retirement.md`](../runbooks/version-gate-retirement.md).
- **Telemetry.** [`telemetry.md`](../telemetry.md) covers the OpenTelemetry
  surface and the `metrics-rs` adapter recipe.
- **Search attributes.** [`search-attributes.md`](../search-attributes.md)
  explains how to index workflows for filtered queries.
- **Architecture.**
  [`autumn-workflow-architecture.md`](../autumn-workflow-architecture.md) and the
  [ADRs](../adr/) document the design decisions behind the engine.
- **Comparison.** [`comparison.md`](../comparison.md) positions harvest against
  Temporal, DBOS, Inngest, Hatchet, and Restate — every harvest claim linked to
  shipped evidence, with an honest section on where harvest is behind.
