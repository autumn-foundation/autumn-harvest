# Migrating from Temporal to Harvest

This guide helps you move a Temporal workflow application to autumn-harvest.
It is for a Rust team that runs Temporal today. It gives an accurate,
itemized answer to one question: what does it cost to port one workflow
type?

Read [`comparison.md`](comparison.md) first, if you have not chosen harvest
yet. That page answers whether you should choose harvest, and why. This
page answers how to move.

Every claim below links to shipped, verifiable evidence. The evidence is an
issue number you can open on GitHub, or a file in this repository. A claim
with no evidence link is a bug in this guide. Please [open an
issue](https://github.com/autumn-foundation/autumn-harvest/issues/new).

## Scope and audience

This guide assumes:

- You write your workflows and activities in Rust today. Or your workflows
  use a non-Rust Temporal SDK. In that case, you plan to rewrite them in
  Rust as part of the move. Harvest ships a Rust SDK only. See
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
  workflow history. It cannot resume one either. The two engines use
  different event models. Temporal's `HistoryEvent` and harvest's
  `WorkflowEvent` are not interchangeable. No conversion between them
  exists. None is planned. A workflow execution that is in flight on
  Temporal at cutover stays on Temporal. It runs to completion there. It
  drains from the Temporal side on its own. Harvest starts only *new*
  executions of the ported workflow type. See the [Dual-run cutover
  playbook](#dual-run-cutover-playbook) for the sequence.
- **Automated code migration (codemods).** You port each workflow function
  by hand. Use the [Concept mapping](#concept-mapping) table and the
  [Workflow-porting checklist](#workflow-porting-checklist).
- **Migration guides for other workflow engines.** This guide covers
  Temporal only. See [`comparison.md`](comparison.md) for how harvest
  compares to DBOS, Inngest, Hatchet, and Restate.
- **Building a feature harvest does not have yet.** The [No equivalent
  yet](#no-equivalent-yet) section names each gap plainly. It does not
  hide any of them.

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
| Blocking wait for one signal, `condition(() => received, timeout?)` | `ctx.wait_for_signal` / `ctx.receive_signal` / `ctx.receive_signal_timeout` | issue #476 (deadline variant); core primitive (plain wait) |
| Blocking wait on many signals or a custom condition, `condition(predicate, timeout?)` | `ctx.await_condition` / `ctx.await_condition_timeout` | core primitive |
| `SignalWithStart` | `signal_with_start_workflow_execution`, `POST /workflows/{name}/signal-with-start` | issue #244 |
| Non-blocking signal check (no first-class Temporal equivalent) | `ctx.try_receive_signal` / `ctx.drain_signals` | issue #775 |
| Duplicate-safe signal delivery (Temporal has no first-class dedup key) | `Idempotency-Key` header on standalone signal delivery | issue #521, issue #753 |

`condition()` in Temporal accepts any predicate over workflow state. It is
not limited to one named signal. A predicate such as `approved ||
cancelled` can read two independent signals in one wait.

Harvest has a direct equivalent. Use `ctx.await_condition` when the
original call took no timeout. Use `ctx.await_condition_timeout` when the
original call took a timeout. Each accepts a closure. The closure reads
local workflow state and returns a `bool`.

Port a multi-signal `condition(predicate, timeout?)` call this way.
Register a push handler for each signal (`ctx.register_signal_handler`,
issue #546). Have each handler set its own flag. Pass a closure that
checks the combined flags to `ctx.await_condition` or
`ctx.await_condition_timeout`. `await_condition_timeout` also takes a
timer ID and a duration in whole seconds. Use the same duration the
original timeout used.

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
| Hand-rolled output/error carryover between scheduled runs | `ctx.last_completion_result()` / `ctx.last_error()` | issue #488 |

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
  region over automatically. Together, these two limits are a real gap for
  a Temporal deployment that relies on global namespace replication for
  disaster recovery.
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
   payloads through a configurable `DataConverter`. Temporal's default
   converter is not plain JSON. It is a chain of converters. Temporal
   tries each payload against the chain in order: a null check, then raw
   bytes, then Protobuf, then JSON as the final fallback for a plain
   object. A `Buffer`, a `Uint8Array`, or a Protobuf message crossing a
   Temporal boundary is not JSON on the wire. This holds even with no
   custom codec configured. List every payload type that crosses each
   activity or workflow boundary before you port it. Harvest serializes
   every input and output as `serde_json::Value` through `serde`. For a
   type that already went through the JSON fallback, add
   `#[derive(serde::Serialize, serde::Deserialize)]`. For a raw-bytes or a
   Protobuf type, write an explicit conversion to a JSON-serializable
   shape first. Convert a raw byte buffer to a base64 string, or to a
   byte array. Convert a Protobuf message to its JSON mapping, through
   the Protobuf library's own JSON codec, or through a hand-written
   struct. Suppose you used a custom Temporal payload codec for
   compression or encryption on top of this chain. Harvest's equivalent
   is a `PayloadCodec`
   ([ADR-0003](adr/0003-payload-codec-event-boundary.md)). The two are not
   the same shape. Port the codec logic, not the wire format.
3. **Translate each retry policy.** Temporal's `RetryPolicy` and harvest's
   `RetryPolicy` cover the same five fields, under different names:

   | Temporal field | Harvest field |
   |---|---|
   | `maximumAttempts` | `max_attempts` |
   | `initialInterval` | `initial_interval` |
   | `backoffCoefficient` | `backoff_coefficient` |
   | `maximumInterval` | `max_interval` |
   | `nonRetryableErrorTypes` | `non_retryable_errors` |

   Check `maximumAttempts` before you copy it. In Temporal, `0` and an
   omitted field both mean unlimited attempts. Harvest has no such value.
   `max_attempts` is always a hard, finite cap. A copy of `0` does not
   give you unlimited retries. It gives you zero. `RetryPolicy::next_delay`
   stops the first retry once the attempt count reaches `max_attempts`, so
   a cap of `0` blocks even that first retry. Leaving the field out is not
   safe either. Harvest's default cap is `3`. Set an explicit
   `max_attempts` for every activity that used Temporal's unlimited-retry
   pattern. If the activity should keep retrying until an outer deadline,
   not until it runs out of attempts, set a high `max_attempts`. Pair it
   with `schedule_to_close` (checklist item 4, below) as the real bound.

   Harvest also supports the same jitter shapes: `JitterPolicy::None`,
   `Full`, `Equal`, and `Decorrelated`. Use these if your Temporal retry
   policy used jittered backoff.
4. **Translate each timeout name.** This is the single most common porting
   mistake. The two engines invert the meaning of two timeout names:

   | Temporal timeout | Bounds | Harvest equivalent |
   |---|---|---|
   | `startToCloseTimeout` | One activity attempt | `#[activity(start_to_close = "...")]` |
   | `scheduleToCloseTimeout` | An activity across all its retries | `#[activity(schedule_to_close = "...")]` (issue #378) |
   | `scheduleToStartTimeout` | Time an activity waits in the queue before a worker claims it | `#[activity(schedule_to_start = "...")]` |
   | `heartbeatTimeout` | Time between activity heartbeats | `#[activity(heartbeat_timeout = "...")]` |
   | **Workflow Run Timeout** — bounds **one run** (one continue-as-new segment) | One workflow execution | `#[workflow(execution_timeout = "...")]` (issue #243) |
   | **Workflow Execution Timeout** — bounds **the whole continue-as-new chain** | Every run from the first start to the final completion | `#[workflow(chain_execution_timeout = "...")]` (issue #617) |

   Read this table carefully. The two engines name the same pair of concepts
   in opposite order. Temporal's *Run* Timeout bounds one run. That is the
   narrow scope. Its *Execution* Timeout bounds the whole continue-as-new
   chain. That is the wide scope. Harvest's naming runs the other way.
   `execution_timeout` is the narrow one. `chain_execution_timeout` is the
   wide one. Match each name against this table. If you skip this check,
   you swap the two timeouts.
5. **Map each Task Queue to a harvest queue.** Temporal's Task Queue is a
   plain string. Both workflow tasks and activity tasks route through it,
   and a worker polls it. Harvest splits the two. An activity's queue comes
   from the `queue` attribute (`#[activity(queue = "...")]`). A workflow's
   own dispatch task uses a separate setting, chosen when you start it: the
   `queue` field on the start request, or `StartWorkflowParams::queue_name`
   from Rust code. Set both. If you omit the workflow-start queue, the
   workflow task lands on `"default"`, even when every one of its
   activities is routed correctly elsewhere. Cover every queue you use, on
   both sides, in the worker's queue list in `WorkerConfig`. Harvest also
   routes within one queue by worker build ID (issue #171) and by
   capability label. Use these if your Temporal deployment used Task Queue
   naming conventions for the same purpose.
6. **Validate with `WorkflowReplayer` and `WorkflowTestEnv` before you ship.**
   `autumn_harvest::testing::WorkflowReplayer` replays a recorded event
   history against your ported workflow function. It reports whether the
   function is deterministic. This is the harvest analogue of Temporal's
   own replay tester. `autumn_harvest::testing::WorkflowTestEnv` drives a
   workflow function end-to-end without a database. Use it to assert the
   function's behavior. It checks which activities the function calls, and
   what it returns, as a plain `#[tokio::test]`. The [Worked
   example](#worked-example) below uses both.

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
   first. A leaf type has no child workflows, and no downstream workflow
   that signals it. Cut over a parent workflow type only after every
   workflow type it spawns as a child already runs live on harvest.

   `ctx.spawn_child_workflow` only starts another harvest execution. It
   cannot start, signal, or await a Temporal execution. A ported parent
   cannot simply tolerate a child type still on Temporal, because nothing
   in harvest can reach that child at all. If a parent must cut over
   before every one of its children does, replace each such child spawn
   with an explicit bridge instead. Write an ordinary harvest activity
   that calls Temporal's own client API. Have it start the child, then
   poll for the child's result, or await a callback for it. Give that
   bridge activity the same idempotency key discipline from step 4,
   below, so a retried bridge call cannot start the same Temporal child
   twice. Propagate a harvest cancellation into a Temporal cancel request
   through the same bridge. Building this bridge is real engineering
   work. It is not a compatibility check. Prefer the dependency order
   above whenever you can.
4. **Make every shared downstream activity idempotent.** During the cutover
   window, both engines may call the same downstream service for different
   executions. Examples are a payment gateway, an email provider, and a
   database write. Give every such activity a stable, request-scoped
   idempotency key.

   Derive that key before you route the request to either engine. Do not
   mint it inside the workflow. A retry of the same logical request can
   land on a different engine than the first attempt did, once its type's
   routing flag flips. `ctx.new_uuid()` (issue #384) is deterministic only
   within one harvest execution's own recorded history. It cannot match a
   key a separate Temporal execution already used for the same request. So
   pass one caller-supplied key into both engines' version of the activity
   instead. Reuse the request key you already carry in Temporal, or the
   request ID your upstream caller (a webhook, an API gateway) already
   assigns. Reserve `ctx.new_uuid()` for a value that never needs to
   survive a retry across engines, such as a correlation ID scoped to one
   harvest run.
5. **Watch both engines with the same discipline during the window.** Keep
   your existing Temporal dashboards running. Stand up the equivalent
   harvest dashboards before you flip the first flag. On the harvest side,
   watch the `harvest.workflow.terminal` counter for the ported type's
   completed/failed/cancelled rate. Also watch its dead letter queue (DLQ)
   for tasks that ran out of retries. On the Temporal side, watch the
   open-workflow count for that type through Temporal's own visibility API.
   It should trend to zero as harvest takes over new starts.
   [`comparison.md`](comparison.md) covers the wider observability surface.
   See also [`telemetry.md`](telemetry.md) for the OpenTelemetry surface,
   and [`docs/dashboards/`](dashboards/) for the starter Grafana pack. A
   per-type cutover means each type's error rate and latency stay directly
   comparable across the two engines for the whole window.
6. **Keep a rollback path per type.** A flag flipped back sends new starts
   to Temporal again. An in-flight harvest execution of that type still runs
   to completion on harvest. The flip does not migrate it back, for the same
   reason history import does not run forward. Do not flip a type's flag
   back once you deregister its Temporal worker code. Keep that worker
   deployed, even if idle, until you are certain you will not roll back.

   This rollback only routes new work. It does not send an
   already-cut-over execution back to Temporal. That gap rarely matters
   for a short-lived workflow type, since its harvest execution finishes
   on its own soon after the flag flips back. It matters for a long-lived
   entity type. Such an execution can loop forever through `continueAsNew`
   and never finish on its own, so this rollback cannot reach it. See the
   reverse handoff at the end of step 7, below, for what to do instead.
7. **Retire the Temporal worker for a type only after its queue is empty on
   both sides.** Confirm zero in-flight Temporal executions of that type.
   Temporal's own visibility API can confirm this count. Confirm zero
   pending harvest starts still routed to Temporal for it. Then remove that
   type's Temporal worker code.

   A long-lived entity workflow may never reach zero on its own. It loops
   forever through `continueAsNew`, and nothing inside that loop ever
   completes it. The [Worked example](#worked-example) below is exactly
   this shape: it keeps renewing a subscription until a `cancel` signal
   arrives. Do not wait for such an execution to drain.

   Do not take a live snapshot of its state. Do not start harvest from
   that snapshot either. A query against a running execution can go stale
   immediately. The execution can still advance after you read it. It can
   process a new signal. It can finish an activity that changes its
   state. Cancellation does not close this gap. Temporal delivers a
   cancellation as an interruption at the workflow's next await point. It
   does not stop an activity already in flight. It does not undo a side
   effect that activity already committed. Either race can start harvest
   from a stale value. It can repeat a side effect the old execution
   already performed. In the worked example, that means charging the same
   billing cycle twice.

   Hand the state across through the workflow's own signal instead. Use
   the same `cancel` signal shown below. Send it. Wait for the execution
   to reach `COMPLETED` on its own. It reaches that state at the point in
   its loop where it already checks the signal. Only then read its final
   state. The [Worked example](#worked-example) below shows one way. Each
   `if (cancelled)` branch returns the state where harvest must resume.
   The two branches differ. The first branch returns the input unchanged.
   Nothing happened yet this run. The second branch returns the next
   cycle, not the current cycle. `chargeCard` already ran for the current
   cycle before that branch's check. Returning the current cycle from
   that branch would charge it again. If your own workflow returns
   nothing useful, query the now-closed execution for its state instead.
   A closed execution cannot advance further. Neither read method can go
   stale. A query against a still-running execution can. Start the
   harvest execution only after that read, carrying the result forward as
   its input. The defined point can sit behind a long wait, such as the
   worked example's 30-day timer. The handoff then takes that long too.
   Accept that wait. Or, before you begin this type's cutover, change the
   workflow to race the wait against the signal instead. Harvest's
   `ctx.receive_signal_timeout` (issue #476) is the primitive for that
   race on the harvest side.

   Treat each entity's handoff as a deliberate cutover step, not a bulk
   migration. Each one is a live, stateful run. It is not disposable
   work.

   Build a reverse handoff only if a long-lived entity's cutover must
   roll back. Mirror the forward handoff above, in the other direction.
   Send the entity's `cancel` signal to its harvest execution. Wait for
   that execution to reach `COMPLETED`. Then read its final state. Read
   it from the execution's own return value, if the ported workflow's
   cancellation branches carry one. This guide's own worked example does
   not. Both of its cancellation branches return `()`. The worked example
   only demonstrates the forward direction. Read the final state instead
   from a query handler against the closed execution (issue #612). This
   is the same fallback named above for a workflow whose return value
   carries nothing useful. Start a fresh Temporal execution from that
   read. This guide does not ship this reverse handoff. You build it
   only if you need one. Decide this before you cut over a long-lived
   entity type. Confirm you can accept harvest as that entity's
   permanent home, if its cutover does not go well.

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
  subscriptionId: string;
  cycles: number;
}

export async function subscriptionRenewal(
  state: SubscriptionState,
): Promise<SubscriptionState | void> {
  let cancelled = false;
  setHandler(cancelSignal, () => {
    cancelled = true;
  });

  // A cancellation already recorded before this run started stops the loop
  // immediately. Nothing happened yet this run, so the checkpoint below is
  // the input unchanged.
  if (cancelled) {
    return state;
  }

  // Derive the key from state.subscriptionId, not from
  // workflowInfo().workflowId. Playbook step 4 needs a key the caller
  // supplies before routing the request to either engine. subscriptionId
  // is that key: it names the same subscription on both sides of a
  // dual-run cutover, no matter which engine's own internal id the request
  // happens to carry.
  const idempotencyKey = `${state.subscriptionId}-cycle-${state.cycles}`;
  await chargeCard(idempotencyKey, state.cycles);

  // Wait for the next billing cycle. A cancel signal delivered during this
  // wait is observed as soon as the wait resolves.
  await sleep('30 days');

  // chargeCard already ran for this cycle, above. The checkpoint below must
  // be the next cycle, not the current one, or a resumed run repeats the
  // charge that already happened.
  if (cancelled) {
    return { subscriptionId: state.subscriptionId, cycles: state.cycles + 1 };
  }

  await continueAsNew<typeof subscriptionRenewal>({
    subscriptionId: state.subscriptionId,
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
    pub subscription_id: String,
    pub cycles: u32,
}

/// A downstream idempotency key, plus the cycle number it applies to. See
/// playbook step 4 in the migration guide: every side-effecting activity
/// call needs its own caller-supplied key, derived from state already in
/// history, so a retried attempt cannot charge the card twice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargeRequest {
    pub idempotency_key: String,
    pub cycles: u32,
}

#[activity(start_to_close = "30s")]
async fn charge_card(_ctx: &ActivityContext, req: ChargeRequest) -> Result<(), String> {
    // A real billing call goes here, passing req.idempotency_key through to
    // the payment provider's own idempotency-key parameter.
    println!(
        "charged card for cycle {} (idempotency key: {})",
        req.cycles, req.idempotency_key
    );
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
    // below, since nothing else runs first. See the module docs above.
    let _ = ctx.system_now();

    // A cancellation already recorded before this run started stops the
    // loop immediately.
    if cancelled.load(Ordering::SeqCst) {
        return Ok(());
    }

    // Derive the key from `subscription_id`, not from `ctx.workflow_id()` or
    // the execution id. Playbook step 4 in the migration guide needs a key
    // the caller supplies before routing the request to either engine.
    // `subscription_id` is that key: it names the same subscription on both
    // sides of a dual-run cutover, no matter which engine's own internal id
    // the request happens to carry.
    let idempotency_key = format!("{}-cycle-{}", state.subscription_id, state.cycles);
    let _: () = ctx
        .execute_activity(
            &charge_card_info(),
            ChargeRequest {
                idempotency_key,
                cycles: state.cycles,
            },
        )
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

    let next = SubscriptionState {
        subscription_id: state.subscription_id.clone(),
        cycles: state.cycles + 1,
    };
    ctx.continue_as_new(serde_json::to_value(next).map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?;

    unreachable!("continue_as_new suspends the run and never resolves")
}
```

### What changed, and why

- **Signal handler.** Temporal's `setHandler(cancelSignal, () => { ... })` is
  push-based and fire-and-forget. It fires whenever a matching signal
  arrives, at any point in the workflow body. Harvest's direct 1:1 port is
  `ctx.register_signal_handler_raw` (issue #546). It is also push-based, and
  also fire-and-forget. Do not reach for `wait_for_signal` here. That is
  harvest's *pull*-based primitive. It matches Temporal's `condition()`
  helper instead. It blocks one code point, rather than reacting from
  anywhere in the workflow body. See the [Signals](#signals) row above.
- **Dispatch timing.** Temporal's `setHandler` callback fires as soon as the
  signal arrives, mid-await. Harvest's push handler dispatches only on the
  *next* history-consulting call the workflow body makes. A signal recorded
  before this run even started needs a flush point before the first
  `cancelled` check, since nothing else has run yet. The port adds
  `ctx.system_now()` for exactly this. It is a deterministic primitive
  call, not an activity or a timer. It records one event on the first live
  pass. Every later replay reads that event back, at no extra cost. A
  signal that arrives during the later `ctx.timer(...)` wait needs no such
  extra call. That wait is itself the flush point. It dispatches the
  handler the moment it resolves. See the module documentation in the
  worked example file for the full dispatch-timing contract.
- **Activity dispatch.** `proxyActivities` plus a plain function call
  becomes `ctx.execute_activity(&charge_card_info(), input)`. The
  `#[activity]` macro generates the `charge_card_info()` function. It
  carries the `start_to_close` timeout, and any retry policy. You never
  pass those by hand at the call site.
- **Idempotency key.** Both sides derive a request-scoped key from
  `subscriptionId` (`subscription_id` in Rust), right before the billing
  call. That field carries the entity's own stable id, not the workflow or
  execution id Temporal or harvest happens to assign this particular run.
  See playbook step 4, above: the key must come from state the caller
  supplies before routing the request to either engine, so a re-route
  during a dual-run cutover cannot mint a fresh key and let a retried
  charge through twice.
- **The timer.** `sleep('30 days')` becomes `ctx.timer(id, seconds)`. Every
  harvest timer needs a stable `id` string. Temporal's sleep needs no name.
  Reuse a descriptive constant, as in `"next-billing-cycle"` above.
- **`continueAsNew`.** Both sides carry the incremented `cycles` value
  forward. Both sides reset the event history. Harvest's `continue_as_new`
  takes a `serde_json::Value`. You serialize the state explicitly at the
  call site.

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

- [`comparison.md`](comparison.md) explains whether you should choose
  harvest over Temporal, DBOS, Inngest, Hatchet, or Restate, and why. It
  uses the same evidence-linked standard as this page.
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
