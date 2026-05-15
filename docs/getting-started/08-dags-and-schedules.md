# Chapter 8 — DAGs and schedules

[← Reliability knobs](07-reliability-knobs.md) · [Index](README.md) · [Next: Operating the service →](09-operations.md)

---

Workflows are the right shape when one orchestration drives a sequence of
steps tied to a single business event (one checkout, one signup, one
upload). Some work isn't shaped like that — it's a *graph* of activities
with fan-out, fan-in, and conditional rerun rules, run on a cron. Think
nightly reconciliation, ETL pipelines, daily report generation.

Harvest models that with **DAGs**: a directed acyclic graph of activities
declared with the `#[dag]` macro, scheduled by the engine, and executed
through the same task queue as everything else.

## Declaring a DAG

```rust
use std::time::Duration;
use autumn_harvest::prelude::*;

#[activity(start_to_close = "5m", queue = "ops")]
async fn export_billing_events(_ctx: &ActivityContext, _input: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    // … pull yesterday's events into the warehouse
    Ok(serde_json::json!({ "rows": 12_345 }))
}

#[activity(start_to_close = "10m", queue = "ops")]
async fn reconcile_gateway(_ctx: &ActivityContext, _input: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    // … diff our records against the payment gateway
    Ok(serde_json::json!({ "discrepancies": 0 }))
}

#[activity(start_to_close = "1m", queue = "ops")]
async fn notify_finance(_ctx: &ActivityContext, _input: serde_json::Value)
    -> HarvestResult<serde_json::Value>
{
    // … email finance with the result, success or failure
    Ok(serde_json::Value::Null)
}

#[dag(
    schedule = "0 6 * * *",       // every day at 06:00
    catchup = false,              // skip missed runs after downtime
    max_active_runs = 1,          // never overlap two runs of this DAG
    default_queue = "ops",
)]
pub fn billing_reconciliation(dag: &mut DagBuilder) {
    let export = dag.activity(export_billing_events);

    let reconcile = dag
        .activity(reconcile_gateway)
        .upstream(&export)
        .retry(RetryPolicy::fixed(3, Duration::from_secs(30)));

    let _notify = dag
        .activity(notify_finance)
        .upstream(&reconcile)
        .trigger_rule(TriggerRule::AllDone);
}
```

That's the whole vocabulary. Three things to notice:

- **`#[dag]` is on a `pub fn`, not `async fn`.** The function describes the
  graph; it does not execute it. The engine calls it once at registration
  to build the `DagDefinition`, then runs that definition on each scheduled
  tick.
- **`dag.activity(f)` returns a `DagTaskRef`.** That handle exposes
  `.upstream(&other)`, `.trigger_rule(...)`, `.retry(...)`,
  `.start_to_close(...)`, and `.queue(...)` for chaining task-level
  overrides. Activity-level attributes from `#[activity]` are inherited as
  defaults and can be overridden per task.
- **`notify_finance` runs even if `reconcile` fails.** That's what
  `TriggerRule::AllDone` means: fire when every upstream has reached a
  terminal state, regardless of outcome. Useful for end-of-pipeline
  notification and cleanup.

## Under the hood — unified execution

Since Harvest 0.3 (`unified-dag-execution` feature, on by default), `#[dag]`
functions are executed as *workflows* on the standard workflow execution path
rather than through a bespoke DAG executor.  The macro lowers the graph
definition into a `WorkflowHandlerFn` that walks `DagDefinition` level by
level and dispatches each activity through `ctx.execute_activity_raw`, so DAG
runs show up as workflow executions in `harvest_workflow_executions`, benefit
from the same replay-safe history model, and are observable through all the
same tooling.

You do **not** need to register the underlying workflow manually —
`HarvestPlugin::dags(dags![my_dag])` auto-registers the `WorkflowInfo` and
(if the DAG has a `schedule = "..."` attribute) the `WorkflowSchedule` for
you.

## `#[dag]` attributes

| Key | Default | Meaning |
|---|---|---|
| `schedule` | none (manual) | Cron expression — `"0 6 * * *"`, `"*/15 * * * *"`. Omit for manual-trigger-only DAGs. |
| `catchup` | `false` | If `true`, the scheduler enqueues a run for every interval missed during downtime. If `false`, only the next-scheduled run runs after a gap. |
| `max_active_runs` | `1` | Cap on concurrent runs of the same DAG. Set higher for fast-cadence DAGs whose runs can safely overlap. |
| `default_queue` | `"default"` | Queue assigned to tasks that don't override it via `#[activity(queue = ...)]` or `.queue(...)`. |

## Trigger rules

`TriggerRule` decides whether a downstream task fires given the terminal
states of its upstream tasks:

| Rule | Fire when |
|---|---|
| `AllSuccess` *(default)* | Every upstream succeeded. |
| `AllDone` | Every upstream reached a terminal state, success or failure. |
| `OneSuccess` | At least one upstream succeeded. |
| `OneFailed` | At least one upstream failed. |
| `AllFailed` | Every upstream failed. |
| `Manual` | Never auto-fire — the operator triggers the task explicitly. |

`AllDone` is the right choice for notification, cleanup, and metric-emit
tasks. `OneSuccess` is the "fan-in for any successful branch" shape.
`OneFailed` is the "alert on first failure" shape.

## Registering DAGs with the plugin

```rust
HarvestPlugin::new()
    .workflows(workflows![checkout, issue_invoice])
    .activities(activities![
        export_billing_events,
        reconcile_gateway,
        notify_finance,
    ])
    .dags(dags![billing_reconciliation])
    .worker(WorkerConfig::default())
    .api("/api/harvest")
```

The activities used by the DAG must also be registered with `activities![]`
— a DAG references activities by name and dispatches them through the same
worker fleet as your workflow code.

## Triggering and managing DAG runs

The dashboard shows each DAG with its schedule, last run, next run, and a
graph view. The CLI and HTTP routes give you operator control:

```bash
harvest dag list
harvest dag trigger billing_reconciliation \
  --conf-json '{"date":"2026-05-07"}'
harvest dag pause billing_reconciliation
```

Or directly:

```bash
curl -s -X POST \
  http://localhost:3000/api/harvest/dags/billing_reconciliation/trigger \
  -H 'Content-Type: application/json' \
  -d '{"conf":{"date":"2026-05-07"}}' | jq .
```

Pausing a DAG keeps the definition registered but stops the scheduler from
firing it; manual triggers still work. Resume by patching it back to active
through the same management route.

## Workflow schedule vs DAG — which one?

| Use a **workflow schedule** when… | Use a **DAG** when… |
|---|---|
| The work is one ordered sequence with a clear linear shape. | The work is a graph: fan-out, fan-in, parallel branches. |
| You need signals, durable timers, child workflows, or version gates inside the run. | The run is purely activity orchestration with no human-wait or signal handoff. |
| Failure handling is per-step compensation (saga). | Failure handling is per-task trigger rules (AllDone, OneFailed). |
| You want to query state mid-run. | The graph is fixed and you want the dashboard's graph view. |

Both are scheduled the same way (cron expression on the registration), both
go through the same task queue, both record audit events. Pick the shape
that matches the work.

## Inspecting DAGs before they run

The engine ships three offline analysis tools that don't need a database
or running service. They're handy in CI or during DAG design.

- **Linter** (`autumn_harvest::dag_linter`) — flags missing retry policies,
  missing timeouts, and excessive parallelism in a `DagDefinition`. Good
  CI gate before merging a DAG change.
- **Simulator** (`autumn_harvest::dag_simulator`) — runs the DAG against
  per-activity mocks and returns each task's terminal status. Use it to
  verify trigger-rule wiring without a Postgres roundtrip.
- **Profiler** (`autumn_harvest::dag_profiler`) — given mock durations per
  activity, reports the critical path and wall-clock estimate.
- **Mermaid / DOT export** (`autumn_harvest::dag_export::export_mermaid`,
  `export_dot`) — render a DAG to a graph diagram for design review.

These are all built on the same `DagDefinition` your `#[dag]` function
produces, so they cost nothing extra to wire up — you already have the
definition in hand at test time.

---

[← Reliability knobs](07-reliability-knobs.md) · [Index](README.md) · [Next: Operating the service →](09-operations.md)
