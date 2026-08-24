# Migrating from Temporal to Harvest

This guide helps you move a Temporal workflow application to autumn-harvest.
It is for a Rust team that runs Temporal today. It gives an accurate,
itemized answer to one question: what does it cost to port this workflow?

Read [`comparison.md`](comparison.md) first, if you have not chosen harvest
yet. That page answers *why* you should choose harvest, or *whether* you
should. This page answers *how* to move.

Every claim below links to shipped, verifiable evidence. The evidence is an
issue number you can open on GitHub, or a file in this repository. A claim
with no evidence link is a bug in this guide. Please [open an
issue](https://github.com/autumn-foundation/autumn-harvest/issues/new).

## Scope and audience

This guide assumes:

- You write your workflows and activities in Rust today. Or your workflows
  use a non-Rust Temporal SDK, and you plan to rewrite them in Rust as part
  of the move. Harvest ships a Rust SDK only. See
  [No equivalent yet](#no-equivalent-yet).
- You want to port workflow *types* one at a time, not all at once. The
  [Dual-run cutover playbook](#dual-run-cutover-playbook) below shows how.
- You already know your own Temporal workflow code. This guide does not
  teach Temporal concepts from scratch. It maps each concept to its harvest
  equivalent.

Use this guide in three steps:

1. Read the [Concept mapping](#concept-mapping) table. Find each Temporal
   primitive your workflows use.
2. Run the [Workflow-porting checklist](#workflow-porting-checklist) against
   one workflow type. Estimate the cost from the answers.
3. Follow the [Dual-run cutover playbook](#dual-run-cutover-playbook) to move
   that workflow type to production without downtime.

## Non-goals

This guide does **not** cover:

- **Automatic Temporal history import.** Harvest cannot read a Temporal
  workflow history and resume it. The two engines use different event
  models. Temporal's `HistoryEvent` and harvest's `WorkflowEvent` are not
  interchangeable. No conversion between them exists. None is planned. A
  workflow execution that is in flight on Temporal at cutover stays on
  Temporal. It runs to completion there and drains from the Temporal side
  on its own. Harvest starts only *new* executions of the ported workflow
  type. See the [Dual-run cutover playbook](#dual-run-cutover-playbook) for
  the sequence.
- **Automated code migration (codemods).** You port each workflow function
  by hand. Use the [Concept mapping](#concept-mapping) table and the
  [Workflow-porting checklist](#workflow-porting-checklist).
- **Migration guides for other workflow engines.** This guide covers
  Temporal only. See [`comparison.md`](comparison.md) for how harvest
  compares to DBOS, Inngest, Hatchet, and Restate.
- **Building a feature harvest does not have yet.** The [No equivalent
  yet](#no-equivalent-yet) section names each gap plainly. It does not paper
  over any of them.

## Concept mapping

Each table below covers one area. "Harvest equivalent" names the Rust
method, HTTP route, or CLI command you reach for. "Reference" names the
issue or file where that capability shipped. Use the reference to verify
the claim yourself.

### Workflows and activities

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| `@workflow.defn` (workflow definition) | `#[workflow]` | Core primitive (Phase 1) |
| `@activity.defn` (activity definition) | `#[activity]` | Core primitive (Phase 1) |
| `proxyActivities` / activity options (timeouts, retry) | `#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, ...))]` | See the [timeout-name translation](#timeout-name-translation) table below |

### Signals

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| Signal handler, `defineSignal` + `setHandler` (fire-and-forget, push) | `ctx.register_signal_handler` / `register_signal_handler_raw` | issue #546 |
| Blocking wait, `condition(predicate, timeout?)` | `ctx.wait_for_signal` / `ctx.receive_signal` / `ctx.receive_signal_timeout` | issue #476 (deadline variant); core primitive (plain wait) |
| `SignalWithStart` | `signal_with_start_workflow_execution`, `POST /workflows/{name}/signal-with-start` | issue #244 |
| Non-blocking signal check (no first-class Temporal equivalent) | `ctx.try_receive_signal` / `ctx.drain_signals` | issue #775 |
| Duplicate-safe signal delivery (Temporal has no first-class dedup key) | `Idempotency-Key` header on standalone signal delivery | issue #521, issue #753 |

### Queries and updates

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| `@workflow.query` | `ctx.register_query_handler`, `#[query]` | issue #234, issue #346 |
| `@workflow.update` | `ctx.register_update_handler`, `#[update]` | issue #140, issue #346 |
| `UpdateWithStart` (`WithStartWorkflowOperation`) | `update_with_start_workflow_execution` | issue #479 |

### Timers and continue-as-new

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| `sleep(duration)` | `ctx.timer(id, seconds)` | Core primitive |
| Manual "sleep until a date" math | `ctx.sleep_until(id, deadline)` | issue #749 |
| Hand-rolled cancellable/renewable wait (no first-class Temporal primitive) | `ctx.start_timer` / `TimerHandle` | issue #768 |
| `continueAsNew()` | `ctx.continue_as_new(input)` | Core primitive |
| Same-workflow-type-only continuation (Temporal requires this) | `ctx.continue_as_new_as` / `ctx.continue_as_new_as_type` (cross-type continuation) | issue #803 |
| Hand-rolled memo/search-attribute carryover between scheduled runs | `ctx.last_completion_result()` / `ctx.last_error()` | issue #488 |

### Child workflows

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| `workflow.executeChild()` | `ctx.spawn_child_workflow` | Core primitive |
| `ParentClosePolicy` (`ABANDON` / `REQUEST_CANCEL` / `TERMINATE`) | `ParentClosePolicy` | issue #347 |
| Hand-rolled `Promise.all` fan-out over children | `ctx.spawn_child_workflow_fan_out` (and `_collect`, `_raw` variants) | issue #601 |
| Hand-rolled `Promise.race` | `ctx.race()` | issue #600 |
| Hand-rolled child-vs-timer race | `ctx.execute_child_workflow_timeout` (the child-or-deadline wait) | issue #779 |

### Schedules

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| Temporal Schedules API (cron / interval spec) | `WorkflowSchedule` | Core scheduler |
| Schedule Overlap Policy | `OverlapPolicy` | issue #241 |
| Schedule catchup window | `CatchupPolicy` | issue #484 |
| Bounded schedule actions (`end_at`, action limit) | `WorkflowSchedule::end_at` / `max_runs` (bounded schedule runs) | issue #478, issue #543 |
| Calendar-aware skip / backfill | `Calendar`, the backfill runner | issue #337 |
| Hand-rolled business-day math | `ctx.timer_business_days` | issue #806 |
| Schedule jitter | `WorkflowSchedule::with_jitter` | issue #240 |
| `updateSchedule` | `PATCH /admin/schedules/{id}` (in-place schedule update) | issue #771 |
| `describeSchedule` run history | `GET /admin/schedules/{id}/runs` | issue #534 |

### Versioning and determinism

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| `GetVersion` | `ctx.version(id, min, max)` | Core primitive (Phase 2, design decision DD-3) |
| `Patched` / `DeprecatePatch` | `ctx.patched(id)` / `ctx.deprecate_patch(id)` | issue #687 |
| `workflow.SideEffect` | `ctx.system_now()`, `ctx.new_uuid()`, `ctx.random_*()`, `ctx.side_effect()` | issue #384 |
| No built-in non-determinism linting | Compile-time determinism guardrails HVG001–HVG011, the `det_check` static analyzer | issue #785 (HVG011), issue #600 / issue #799 (HVG010) |
| Replay testing for safe deploys (`WorkflowReplayer`, same name in the Temporal .NET and PHP SDKs) | `autumn_harvest::testing::WorkflowReplayer` | `autumn-harvest/tests/replayer_tests.rs`, Phase 3.5 |

### Worker placement and build routing

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| Local Activity | `#[activity(local = true)]` | issue #98 |
| Worker Build ID Versioning (Worker Deployment Versions) | Worker build-ID routing | issue #171 |
| Gradual version rollout | Percentage-based build ramp | issue #604 |

### Operational lifecycle

| Temporal primitive | Harvest equivalent | Reference |
|---|---|---|
| Worker Sessions (Go SDK `workflow.CreateSession`) | `ctx.create_session()` | issue #606 |
| Reset Workflow Execution | Workflow reset (event-ID boundary), DAG retry-from-failed-node | issue #148, issue #366 |
| Terminate (forceful) | `POST /workflows/{id}/terminate` | issue #504 |
| Cancel (cooperative) | `POST /workflows/{id}/cancel`, `ctx.is_cancelled()` | Core primitive |
| Workflow Pause (experimental, CLI-only in Temporal) | Per-execution pause/resume (stable, reversible, SDK-level) | issue #383, issue #609 |
| Search Attributes / Visibility | Search attributes | [`search-attributes.md`](search-attributes.md) |
| Durable, named cross-workflow mutual exclusion (no first-class Temporal primitive) | `ctx.mutex(key)` | issue #691 |

## No equivalent yet

Three Temporal capabilities have no harvest equivalent today. This section
names each gap plainly. It does not imply a workaround exists.

- **Nexus.** Nexus gives cross-namespace and cross-cluster RPC between
  independently deployed workflows. Harvest has no cross-service RPC
  primitive. Every primitive in the [Concept mapping](#concept-mapping)
  table above operates within one harvest deployment. Suppose your Temporal
  application uses Nexus to call into a separately owned namespace. That
  call has no direct harvest port. Use an ordinary API call or a message
  queue instead of a durable RPC.
- **Multi-region / global namespaces.** Harvest runs against one or more
  Postgres shards. See [`sharding.md`](sharding.md). Harvest does not
  replicate workflow state across geographic regions. It does not fail a
  region over automatically. This is a real gap for a Temporal deployment
  that relies on global namespace replication for disaster recovery.
- **Non-Rust SDKs.** Harvest ships a Rust SDK only. Suppose your Temporal
  workflows are in Go, TypeScript, Java, Python, .NET, or PHP. You rewrite
  them in Rust to move to harvest. No bridge or interop layer exists.

See [`comparison.md`](comparison.md#where-harvest-is-behind) for the current
state of these gaps, and other gaps, including which ones the team plans to
close.

## Workflow-porting checklist

Run this checklist against one workflow type before you port it. Each item
below is a concrete task, not a vague warning.

1. **Run the determinism guardrails.** Compile your ported code. Harvest's
   proc-macro lint (HVG001–HVG011) checks each workflow body. It rejects a
   direct call to the wall clock. It rejects non-deterministic randomness. It
   rejects an inline read of a process environment variable. It rejects a
   spawned background task, direct I/O, and a mutation of process-global
   state. It rejects an iteration of a `HashMap`/`HashSet` inside a
   command-emitting loop. Temporal relies on documentation and review
   discipline for these same rules. Harvest turns most of them into a
   compile error. Fix each flagged call site. Reach for the matching
   deterministic primitive in the [Concept mapping](#concept-mapping) table.
   Most often, that primitive is `ctx.system_now()`, `ctx.new_uuid()`, or
   `ctx.side_effect()`.
2. **Map your payload types.** Temporal serializes activity and workflow
   payloads through a configurable `DataConverter`. The default converter is
   JSON, with optional compression or encryption codecs. Harvest serializes
   every input and output as `serde_json::Value` through `serde`. Add
   `#[derive(serde::Serialize, serde::Deserialize)]` to each type that
   crossed a Temporal activity or workflow boundary. Suppose you used a
   custom Temporal payload codec for compression or encryption. Harvest's
   equivalent is a `PayloadCodec` (issue #608). The two are not the same
   shape. Port the codec logic, not the wire format.
3. **Translate each retry policy.** Temporal's `RetryPolicy` and harvest's
   `RetryPolicy` cover the same five fields, under different names:

   | Temporal field | Harvest field |
   |---|---|
   | `maximumAttempts` | `max_attempts` |
   | `initialInterval` | `initial_interval` |
   | `backoffCoefficient` | `backoff_coefficient` |
   | `maximumInterval` | `max_interval` |
   | `nonRetryableErrorTypes` | `non_retryable_errors` |

   Copy each value across. Harvest also supports the same jitter shapes:
   `JitterPolicy::None`, `Full`, `Equal`, and `Decorrelated`. Use these if
   your Temporal retry policy used jittered backoff.
4. **Translate each timeout name.** This is the single most common porting
   mistake. Two names are inverted between the engines:

   | Temporal timeout | Bounds | Harvest equivalent |
   |---|---|---|
   | `startToCloseTimeout` | One activity attempt | `#[activity(start_to_close = "...")]` |
   | `scheduleToCloseTimeout` | An activity across all its retries | `#[activity(schedule_to_close = "...")]` (issue #378) |
   | `scheduleToStartTimeout` | Time an activity waits in the queue before a worker claims it | `#[activity(schedule_to_start = "...")]` |
   | `heartbeatTimeout` | Time between activity heartbeats | `#[activity(heartbeat_timeout = "...")]` |
   | **Workflow Run Timeout** — bounds **one run** (one continue-as-new segment) | One workflow execution | `#[workflow(execution_timeout = "...")]` (issue #243) |
   | **Workflow Execution Timeout** — bounds **the whole continue-as-new chain** | Every run from the first start to the final completion | `#[workflow(chain_execution_timeout = "...")]` (issue #617) |

   Read this table carefully. The two engines name the same pair of concepts
   in opposite order. Temporal's *Run* Timeout bounds one run — the narrow
   scope. Its *Execution* Timeout bounds the whole continue-as-new chain —
   the wide scope. Harvest's naming runs the other way. `execution_timeout`
   is the narrow one. `chain_execution_timeout` is the wide one. A name
   match without a check against this table swaps the two.
5. **Map each Task Queue to a harvest queue.** Temporal's Task Queue is a
   plain string. Both workflow tasks and activity tasks route through it,
   and a worker polls it. Harvest's equivalent is the `queue` attribute
   (`#[activity(queue = "...")]`), plus the worker's queue list in
   `WorkerConfig`. Harvest also routes within one queue by worker build ID
   (issue #171) and by capability label. Use these if your Temporal
   deployment used Task Queue naming conventions for the same purpose.
6. **Validate with `WorkflowReplayer` and `WorkflowTestEnv` before you ship.**
   `autumn_harvest::testing::WorkflowReplayer` replays a recorded event
   history against your ported workflow function. It reports whether the
   function is deterministic — the harvest analogue of Temporal's own
   replay tester. `autumn_harvest::testing::WorkflowTestEnv` drives a
   workflow function end-to-end without a database. Use it to assert the
   function's behavior — which activities it calls, what it returns — as a
   plain `#[tokio::test]`. The [Worked example](#worked-example) below uses
   both.

## Dual-run cutover playbook

Port and cut over one workflow *type* at a time. Do not flip one flag for
your whole application.

1. **Route new starts by workflow type, not by a global switch.** Add a
   feature flag, or a config table keyed by workflow type name. For each
   type, the flag says whether a *new* request starts a Temporal execution
   or a harvest execution. An in-flight execution of either engine keeps
   running on the engine that started it. See the
   [history-import non-goal](#non-goals) above.
2. **Port and validate one workflow type completely before you flip its
   flag.** Run the [Workflow-porting checklist](#workflow-porting-checklist)
   against it. Confirm `WorkflowReplayer` reports no non-determinism against
   a realistic set of recorded, or hand-built, histories. Confirm
   `WorkflowTestEnv` covers its main branches.
3. **Order types by dependency, not by size.** Cut over a leaf workflow type
   first — one with no child workflows, and no downstream workflow that
   signals it. Cut over a parent workflow type only after every workflow
   type it spawns as a child already runs live on harvest. Or cut it over
   after you confirm the parent tolerates a Temporal child.
4. **Make every shared downstream activity idempotent.** During the cutover
   window, both engines may call the same downstream service for different
   executions — a payment gateway, an email provider, a database write. Give
   every such activity a stable, request-scoped idempotency key. Harvest's
   `ctx.new_uuid()` (issue #384) mints one deterministically, if you need to
   generate it inside the activity's owning workflow. Or reuse the
   caller-supplied key you already use in Temporal.
5. **Watch both engines with the same discipline during the window.** Keep
   your existing Temporal dashboards running. Stand up the equivalent
   harvest dashboards before you flip the first flag. [`comparison.md`](comparison.md)
   covers the observability surface. See also [`telemetry.md`](telemetry.md)
   for the OpenTelemetry surface, and [`docs/dashboards/`](dashboards/) for
   the starter Grafana pack. A per-type cutover means each type's error rate
   and latency stay directly comparable across the two engines for the
   whole window.
6. **Keep a rollback path per type.** A flag flipped back sends new starts
   to Temporal again. An in-flight harvest execution of that type still runs
   to completion on harvest. The flip does not migrate it back, for the same
   reason history import does not run forward. Do not flip a type's flag
   back once you deregister its Temporal worker code. Keep that worker
   deployed, even if idle, until you are certain you will not roll back.
7. **Retire the Temporal worker for a type only after its queue is empty on
   both sides.** Confirm zero in-flight Temporal executions of that type —
   Temporal's own visibility API answers this. Confirm zero pending harvest
   starts still routed to Temporal for it. Then remove that type's Temporal
   worker code.

## Worked example

The workflow below ports directly. It has one push-based signal, one
activity, one durable timer, and a `continueAsNew` loop. The Temporal side is
TypeScript. The harvest side is a real, compiling Rust file:
[`examples/temporal_port_subscription_renewal.rs`](../autumn-harvest/examples/temporal_port_subscription_renewal.rs).
It has its own tests. This repository's CI runs those tests on every change.

Both sides implement the same subscription-renewal entity. Charge a
customer. Wait 30 days for the next billing cycle. Loop via `continueAsNew`.
Stop the loop if a `cancel` signal arrived.

### Temporal (TypeScript)

```typescript
import {
  proxyActivities,
  defineSignal,
  setHandler,
  sleep,
  continueAsNew,
} from '@temporalio/workflow';
import type * as activities from './activities';

const { chargeCard } = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 seconds',
});

export const cancelSignal = defineSignal('cancel');

export interface SubscriptionState {
  cycles: number;
}

export async function subscriptionRenewal(
  state: SubscriptionState,
): Promise<void> {
  let cancelled = false;
  setHandler(cancelSignal, () => {
    cancelled = true;
  });

  // A cancellation already recorded before this run started stops the loop
  // immediately.
  if (cancelled) {
    return;
  }

  await chargeCard(state.cycles);

  // Wait for the next billing cycle. A cancel signal delivered during this
  // wait is observed as soon as the wait resolves.
  await sleep('30 days');

  if (cancelled) {
    return;
  }

  await continueAsNew<typeof subscriptionRenewal>({
    cycles: state.cycles + 1,
  });
}
```

### Harvest (Rust)

```rust
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubscriptionState {
    pub cycles: u32,
}

#[activity(start_to_close = "30s")]
async fn charge_card(_ctx: &ActivityContext, cycles: u32) -> Result<(), String> {
    // ... real billing call goes here ...
    println!("charged card for cycle {cycles}");
    Ok(())
}

#[workflow]
async fn subscription_renewal(
    ctx: &WorkflowContext,
    state: SubscriptionState,
) -> Result<(), String> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let handler_flag = cancelled.clone();
    ctx.register_signal_handler_raw("cancel", move |_payload| {
        handler_flag.store(true, Ordering::SeqCst);
    });

    // Registration only stores the handler -- it does not dispatch inline.
    // This cheap deterministic primitive call flushes it before the check
    // below, since nothing else runs first.
    let _ = ctx.system_now();

    // A cancellation already recorded before this run started stops the
    // loop immediately.
    if cancelled.load(Ordering::SeqCst) {
        return Ok(());
    }

    let _: () = ctx
        .execute_activity(&charge_card_info(), state.cycles)
        .await
        .map_err(|e| e.to_string())?;

    // Wait for the next billing cycle. A cancel signal delivered during
    // this wait is dispatched to the handler above as soon as the wait
    // resolves, before the check below runs.
    ctx.timer("next-billing-cycle", 30 * 24 * 60 * 60)
        .await
        .map_err(|e| e.to_string())?;

    if cancelled.load(Ordering::SeqCst) {
        return Ok(());
    }

    let next = SubscriptionState { cycles: state.cycles + 1 };
    ctx.continue_as_new(serde_json::to_value(next).map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}
```

### What changed, and why

- **Signal handler.** Temporal's `setHandler(cancelSignal, () => { ... })` is
  push-based and fire-and-forget. It fires whenever a matching signal
  arrives, at any point in the workflow body. Harvest's direct 1:1 port is
  `ctx.register_signal_handler_raw` (issue #546). It is also push-based, and
  also fire-and-forget. Do not reach for `wait_for_signal` here. That is
  harvest's *pull*-based primitive. It matches Temporal's `condition()`
  helper instead, and it blocks one code point rather than reacting from
  anywhere. See the [Signals](#signals) row above.
- **Dispatch timing.** Temporal's `setHandler` callback fires as soon as the
  signal arrives, mid-await. Harvest's push handler dispatches only on the
  *next* history-consulting call the workflow body makes. A signal recorded
  before this run even started needs a flush point before the first
  `cancelled` check, since nothing else has run yet. The port adds
  `ctx.system_now()` for exactly this. It is a cheap deterministic
  primitive call, not an activity or a timer, so it costs nothing. A signal
  that arrives during the later `ctx.timer(...)` wait needs no such extra
  call. That wait is itself the flush point, and dispatches the moment it
  resolves. See the module documentation in the worked example file for the
  full dispatch-timing contract.
- **Activity dispatch.** `proxyActivities` plus a plain function call
  becomes `ctx.execute_activity(&charge_card_info(), input)`. The
  `#[activity]` macro generates the `charge_card_info()` function. It
  carries the `start_to_close` timeout, and any retry policy. You never
  pass those by hand at the call site.
- **The timer.** `sleep('30 days')` becomes `ctx.timer(id, seconds)`. Every
  harvest timer needs a stable `id` string. Temporal's sleep needs no name.
  Reuse a descriptive constant, as in `"next-billing-cycle"` above.
- **`continueAsNew`.** Both sides carry the incremented `cycles` value
  forward, and reset the event history. Harvest's `continue_as_new` takes a
  `serde_json::Value`. The state is serialized explicitly at the call site.

Read the full worked example file for the embedded tests. One test exercises
the non-cancelled path: the activity runs, and the loop continues. One test
exercises the cancelled path: a signal recorded before the run starts stops
the loop before the activity ever runs. One test calls `.replay_check(...)`.
It asserts the recorded history replays deterministically. Run the tests
with:

```bash
cargo test -p autumn-harvest --no-default-features --features testing \
    --example temporal_port_subscription_renewal
```

## Related

- [`comparison.md`](comparison.md) explains why, or whether, you should
  choose harvest over Temporal, DBOS, Inngest, Hatchet, or Restate. It uses
  the same evidence-linked standard as this page.
- [`docs/getting-started/`](getting-started/) is the from-scratch tutorial.
  Use it if you are learning harvest's primitives for the first time,
  instead of porting existing Temporal code.
- [`docs/getting-started/07-reliability-knobs.md`](getting-started/07-reliability-knobs.md)
  covers retries, concurrency caps, local activities, queues, versioning,
  and search attributes. It goes deeper than the [Workflow-porting
  checklist](#workflow-porting-checklist) above.
- [`docs/getting-started/11-testing.md`](getting-started/11-testing.md)
  covers unit tests for workflow code and `WorkflowReplayer` regression
  coverage.
- [`workflow-determinism-guide.md`](workflow-determinism-guide.md) has the
  full HVG001–HVG011 guardrail catalogue. The [Workflow-porting
  checklist](#workflow-porting-checklist) points to it.
- [`sharding.md`](sharding.md) explains how harvest spreads workflow state
  across Postgres shards. This relates to the [multi-region
  gap](#no-equivalent-yet) noted above.
