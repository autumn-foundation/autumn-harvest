# Chapter 8 — DAGs and schedules

[← Reliability knobs](07-reliability-knobs.md) · [Index](README.md) · [Next: Worker routing and capabilities →](09-worker-routing.md)

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

## Dynamic Task Mapping (Fan-Out)

Sometimes, the width of the graph isn't known at design time. You might query a database for a list of partition IDs, and then want to process each partition in parallel.

Harvest supports **dynamic task mapping** (issue #485). A mapped task maps over an upstream task that returns a JSON array, executing one concurrent instance of the activity for each element in the array.

### Declaring a Mapped Task

Use `dag.map_activity` and chain `.over(&upstream)` to bind the fan-out to the upstream task:

```rust
#[activity]
async fn list_partitions(_ctx: &ActivityContext) -> HarvestResult<Vec<String>> {
    Ok(vec!["p0".into(), "p1".into(), "p2".into()])
}

#[activity]
async fn process_partition(_ctx: &ActivityContext, partition: String) -> HarvestResult<Value> {
    // Process single partition...
    Ok(serde_json::Value::Null)
}

#[activity]
async fn combine_results(_ctx: &ActivityContext, results: Vec<Value>) -> HarvestResult<Value> {
    // Downstream collect task receives the gathered array
    Ok(serde_json::Value::Null)
}

#[dag]
pub fn partition_etl(dag: &mut DagBuilder) {
    // 1. Upstream node produces a JSON array
    let list = dag.activity(list_partitions);

    // 2. Mapped node fans out over the array in parallel
    let process = dag.map_activity(process_partition).over(&list);

    // 3. Downstream collect node receives the array of results
    let _combine = dag.activity(combine_results).upstream(&process);
}
```

### Failure Policies

By default, mapped nodes use the `FailFast` failure policy. You can override it via `.map_failure_policy(...)`:

```rust
let process = dag.map_activity(process_partition)
    .over(&list)
    .map_failure_policy(MapFailurePolicy::CollectAll);
```

| Policy | Behavior | Downstream Input |
|---|---|---|
| `FailFast` *(default)* | Stop execution and cancel in-flight instances on first failure. The mapped task fails. | Downstream does not run (unless trigger rule permits). |
| `CollectAll` | Execute all N instances to completion. Gathers outcomes for all slots into a status array. Mapped task succeeds. | Downstream receives array of outcome objects: `[{"status":"succeeded","value":v}, {"status":"failed","error":"err"}]`. |

### Behavior and Guarantees

- **Empty Arrays (N = 0)**: If the upstream returns `[]`, the mapped task completes immediately as a successful no-op. Downstream collect nodes receive `[]` and still fire.
- **Replay Determinism**: The fanned-out width N is a pure function of recorded upstream outputs. During replay, Harvest validates that the number of scheduled instances matches the runtime length of the array. If N differs on replay, Harvest halts execution with a `NonDeterministic` error.

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

## Data-dependent branching

Trigger rules gate a node on the *state* of its upstreams (succeeded,
failed, skipped). Sometimes you need to route on the upstream's *output
value* — "if `fraud_score > 0.8`, run manual review; otherwise
auto-approve." That's what **condition predicates** do.

Call `.condition(|outputs| …)` on a `DagTaskRef`. The closure receives a
slice of `serde_json::Value` — one element per upstream output, in the
order the upstreams were declared. If it returns `false`, the node is
skipped; the skip propagates downstream through the normal trigger-rule
inference (so an `AllSuccess` child of a skipped node is also skipped, and
an `AllDone` join that receives all skips still fires).

### Fraud-routing example

```rust
use serde_json::Value;
use autumn_harvest::prelude::*;

#[activity(start_to_close = "30s")]
async fn score_payment(ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    // Returns {"score": 0.0–1.0}
    Ok(serde_json::json!({ "score": 0.92 }))
}

#[activity(start_to_close = "5m")]
async fn manual_review(ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(Value::Null)
}

#[activity(start_to_close = "5s")]
async fn auto_approve(ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(Value::Null)
}

#[activity(start_to_close = "30s")]
async fn notify_result(ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(Value::Null)
}

#[dag(schedule = "*/5 * * * *")]
pub fn fraud_routing(dag: &mut DagBuilder) {
    // Step 1: score the payment.
    let score = dag.activity(score_payment);

    // Step 2a: manual review only when score is high.
    let review = dag
        .activity(manual_review)
        .upstream(&score)
        .condition(|outputs| {
            outputs[0]["score"].as_f64().unwrap_or(0.0) > 0.8
        });

    // Step 2b: auto-approve only when score is low.
    let approve = dag
        .activity(auto_approve)
        .upstream(&score)
        .condition(|outputs| {
            outputs[0]["score"].as_f64().unwrap_or(0.0) <= 0.8
        });

    // Step 3: join — AllDone so it fires regardless of which branch ran.
    let _notify = dag
        .activity(notify_result)
        .upstream(&review)
        .upstream(&approve)
        .trigger_rule(TriggerRule::AllDone);
}
```

At each run exactly one of `manual_review` / `auto_approve` executes; the
other is skipped. `notify_result` sees two upstreams — one succeeded, one
skipped — so `AllDone` fires. If you use `AllSuccess` there instead,
`notify_result` would also be skipped (skipped is not succeeded).

### N-way switch

Conditions are plain Rust closures, so multi-way routing is just multiple
tasks with mutually-exclusive predicates:

```rust
#[dag]
pub fn risk_triage(dag: &mut DagBuilder) {
    let score = dag.activity(score_payment);

    let _low = dag.activity(low_risk_path).upstream(&score)
        .condition(|o| o[0]["score"].as_f64().unwrap_or(0.0) < 0.3);

    let _medium = dag.activity(medium_risk_path).upstream(&score)
        .condition(|o| {
            let s = o[0]["score"].as_f64().unwrap_or(0.0);
            (0.3..0.8).contains(&s)
        });

    let _high = dag.activity(high_risk_path).upstream(&score)
        .condition(|o| o[0]["score"].as_f64().unwrap_or(0.0) >= 0.8);
}
```

Exactly one branch runs per execution. The engine evaluates each condition
independently — a task is skipped when *its* condition returns `false`,
regardless of what any sibling condition returned.

### Conditions on mapped tasks

`.condition(…)` is available on `DagMapTaskRef` (returned by
`dag.map_activity(…).over(&upstream)`) and works the same way: if the
condition is false, the entire mapped fan-out is skipped as a unit.

### Determinism rule

The predicate is a **pure function of upstream outputs**. Those outputs are
already frozen in `harvest_events` when the condition runs, so the same
closure call produces the same result on every replay — as long as the
closure only reads the `outputs` slice and nothing else. Do not read
process state, the system clock, or random values inside a condition
closure; use [`ctx.side_effect`](07-reliability-knobs.md) in an upstream
activity instead and read the recorded value through its output.

### Vantage UI and observability

The Vantage DAG detail page distinguishes two skip reasons:

| Display text | Meaning |
|---|---|
| *Skipped (upstream)* | Node skipped because a trigger rule was not satisfied. |
| *Skipped (condition)* | Node skipped because its condition predicate returned `false`. |

A `MarkerRecorded` event named `dag_skip:{N}` (where *N* is the zero-based
task index) is appended to the execution history for every condition-skip.
This event appears on the execution timeline page and can be queried
directly from `harvest_events`. Trigger-rule skips emit no marker, so
pre-existing DAG histories replay unchanged.

### Simulator note

The offline DAG simulator (`autumn_harvest::dag_simulator`) treats all
nodes as runnable for the purpose of structure validation — it does not
evaluate condition closures. Use `WorkflowTestEnv` or a real run to verify
routing behaviour.

## Signal / approval gates

A **gate node** pauses a DAG run until a named signal arrives, then makes the
signal payload its output so downstream nodes can consume it. It's the
declarative way to insert a human approval — or any external-event wait —
*between* graph nodes without rewriting the whole pipeline as a `#[workflow]`.

Gate nodes are **unified-DAG only** (they lower onto the unified
workflow-execution path). A classic DAG containing a gate is rejected at build
time with a `DagSignalGateRequiresUnifiedExecution` error naming the DAG and
signal. Enable the `unified-dag-execution` feature (on by default).

### Declaring a gate

```rust
use std::time::Duration;
use autumn_harvest::dag::GateTimeoutAction;

#[dag(default_queue = "approvals")]
fn order_approval_pipeline(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_order);
    // Pause until the "approval" signal arrives; fail the run if it doesn't
    // arrive within 24h.
    let gate = dag
        .signal_gate_with_timeout(
            "approval",
            Duration::from_secs(24 * 60 * 60),
            GateTimeoutAction::FailRun,
        )
        .upstream(&extract);
    let _load = dag.activity(load_order).upstream(&gate);
}
```

`signal_gate(name)` is the no-timeout form (wait indefinitely).
`signal_gate_with_timeout(name, timeout, on_timeout)` arms a durable deadline.
A gate returns a `DagTaskRef`, so it composes exactly like any other node:
`.upstream(&gate)`, `.map_activity(f).over(&gate)` (fan out over an array
payload), `.condition(...)`.

### Delivering the signal (unblocking a gate)

Deliver the gate's signal with the ordinary standalone signal route — the gate
consumes it just like a `wait_for_signal`:

```bash
curl -X POST \
  http://localhost:8080/api/harvest/workflows/{exec_id}/signal/approval \
  -H 'Content-Type: application/json' \
  -d '{"approved": true, "reviewer": "alice"}'
```

The JSON body becomes the gate's output. A downstream node reads it via a
`.condition(...)` predicate or a `.map_activity(...).over(&gate)` fan-out.

### Timeout: fail vs continue

| `on_timeout`                  | when the deadline fires first | gate output   |
|-------------------------------|-------------------------------|---------------|
| `GateTimeoutAction::FailRun`  | the DAG run **fails**         | —             |
| `GateTimeoutAction::Continue` | the run **continues**         | `Value::Null` |

### "Continue to a named branch" is declarative

There is no bespoke branch-target mechanism. Under `Continue`, the gate's
null-vs-payload output *is* the branch selector — attach a `.condition(...)` to
each downstream node:

```rust
#[dag(default_queue = "approvals")]
fn order_approval_with_fallback(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_order);
    let gate = dag
        .signal_gate_with_timeout(
            "approval",
            Duration::from_secs(24 * 60 * 60),
            GateTimeoutAction::Continue,
        )
        .upstream(&extract);
    // Approved: gate output is the (non-null) signal payload.
    let _fulfil = dag
        .activity(load_order)
        .upstream(&gate)
        .condition(|ups| !ups[0].is_null());
    // Timed out: gate output is the null sentinel.
    let _escalate = dag
        .activity(escalate_to_manager)
        .upstream(&gate)
        .condition(|ups| ups[0].is_null());
}
```

The condition-skipped branch records a `dag_skip:{N}` marker exactly like any
other data-dependent skip, and the run still succeeds.

### Durability and replay

Gates reuse the existing signal/timer machinery — no new `WorkflowEvent`
variant, no migration. A gated run is durable across restarts and replays
deterministically: whichever of the signal or the deadline is recorded first
in history wins on every replay. See `examples/dag_approval_gate.rs` for a
complete, tested walkthrough (signal branch, timeout-fail, and
timeout-escalate, each with a `WorkflowReplayer` self-check).

### MCP exposure

A `#[dag(mcp)]` DAG that contains a gate keeps its `signal_{dag}` MCP tool, so
an agent can unblock the gate by handle; an activity-only DAG suppresses that
tool.

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

## Incremental scheduled jobs — last-completion-result carryover

Scheduled workflows can read the previous run's output without an external
high-water-mark table, using two `WorkflowContext` accessors added in issue #488:

| Accessor | Returns |
|---|---|
| `ctx.last_completion_result::<T>()` | Deserialized output of the most recent *COMPLETED* run of the same schedule; `None` on first run or if no prior run succeeded |
| `ctx.last_error()` | Error string from the most recent *terminal* run if it ended FAILED or TIMED_OUT; `None` when that run COMPLETED (recovery) or for manual starts |

Both values are **resolved once at workflow start** and **frozen into the
`WorkflowStarted` event**, so replay on any worker always returns the same
values without re-querying the database.

```rust
use serde::{Deserialize, Serialize};
use autumn_harvest::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cursor {
    last_processed_id: i64,
}

#[workflow]
async fn incremental_etl(ctx: &WorkflowContext, _: ()) -> Result<Cursor, String> {
    // Read the cursor written by the previous successful run.
    let prior: Option<Cursor> = ctx
        .last_completion_result::<Cursor>()
        .map_err(|e| e.to_string())?;

    // Log when recovering from a previous failure.
    if let Some(err) = ctx.last_error() {
        ctx.logger().warn(&format!("Previous run failed: {err}"));
    }

    let since_id = prior.as_ref().map(|c| c.last_processed_id).unwrap_or(0);
    // … fetch_batch and process rows WHERE id > since_id …
    Ok(Cursor { last_processed_id: since_id + 100 })
}
```

See `autumn-harvest/examples/incremental_etl_schedule.rs` for the full pattern.

### Semantic guarantees

- **First run**: both accessors return `None`.
- **Manual (non-scheduled) start**: both accessors return `None`; the
  `schedule_id` is `None` on the start params, so no carryover is resolved.
- **Skipped fires** (`OverlapPolicy::Skip`): a skipped slot never invokes
  `start_or_load_workflow_execution`, so the next run's `last_completion_result`
  still refers to the last non-skipped COMPLETED run. Skips do not reset or
  advance the cursor.
- **Recovery branch**: `last_completion_result` is the last *COMPLETED* output
  (may be several runs old); `last_error` reflects the single most recent
  *terminal* run and is `None` once that run COMPLETED — or was CANCELLED /
  TERMINATED (a later cancellation masks an older failure rather than
  resurrecting it). Check `last_error()` to know whether the job is still
  recovering.
- **continue-as-new**: a continuation inherits the predecessor's frozen
  carryover (the continuation is the same logical scheduled run), so cursors and
  recovery state survive the fork.
- **Slot ordering**: carryover selects the *previous logical fire* by the
  schedule slot (`scheduled_for`), not by completion time. Overlapping,
  catch-up, or backfilled fires that finish out of order therefore can't hand a
  later run an older slot's output and roll its cursor backward.
- **Non-overlapping assumption**: carryover is designed for the default
  `max_active_runs = 1` / `OverlapPolicy::Skip`. The source is the highest
  *earlier* slot that has reached a terminal state, so if you set
  `max_active_runs > 1` a later slot can start while an earlier slot is still
  running and observe a stale cursor (re-processing that slot's range). Keep
  cursor-style incremental jobs at `max_active_runs = 1`.
- **Backfills**: backfilled runs participate in the schedule's carryover lineage
  (they share the schedule's `schedule_id` and carry their own `scheduled_for`
  slot, so they slot into the lineage at the correct position).
- **Reset**: reset forks are operator interventions and are *excluded* from
  carryover (their `schedule_id` is left `None`) so resetting an old slot cannot
  roll a later run's incremental cursor backward.
- **PII / payload codecs**: the carried-over output copy frozen in
  `WorkflowStarted` is routed through the same payload codec and redacted-history
  allowlist as any other payload, so a configured codec encrypts/redacts it.

## Workflow schedule vs DAG — which one?

| Use a **workflow schedule** when… | Use a **DAG** when… |
|---|---|
| The work is one ordered sequence with a clear linear shape. | The work is a graph: fan-out, fan-in, parallel branches. |
| You need arbitrary signal handlers, durable timers, child workflows, or version gates inside the run. | The run is activity orchestration — including a **single signal/approval gate** between nodes, which a [signal gate](#signal--approval-gates) handles declaratively without dropping to a workflow. |
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

[← Reliability knobs](07-reliability-knobs.md) · [Index](README.md) · [Next: Worker routing and capabilities →](09-worker-routing.md)
