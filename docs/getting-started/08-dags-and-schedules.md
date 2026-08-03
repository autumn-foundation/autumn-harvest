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
| `execution_timeout` | none | Hard wall-clock deadline for the whole DAG run — see below. |
| `sla` | none | Soft SLA for the whole DAG run — see below. |

### Deadlines for scheduled DAG runs (`execution_timeout` / `sla`)

A unified DAG (the default execution mode — see "Under the hood" above) is
just a `#[workflow]` under the hood, so it can declare the same hard
`execution_timeout` and soft `sla` a plain workflow does:

```rust
#[dag(schedule = "0 6 * * *", execution_timeout = "4h", sla = "3h")]
fn nightly_etl(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_users);
    let _load = dag.activity(load_warehouse).upstream(&extract);
}
```

`execution_timeout` and `sla` are propagated **verbatim** onto the DAG's
shadow `WorkflowInfo` (`DagInfo::as_workflow_info`) and enforced by the
**same** scanners a plain `#[workflow(execution_timeout = "…", sla = "…")]`
already uses — the existing execution-timeout scanner
(`timeout::enforce_workflow_execution_timeouts`) and the existing soft-SLA
scanner (`timeout::enforce_workflow_sla_breaches`). There is no separate
DAG-specific deadline mechanism, no new `WorkflowEvent` variant, and no new
migration: a DAG run that overruns `execution_timeout` transitions to
`TIMED_OUT` exactly like an overrun plain workflow, and a run that passes its
`sla` emits `harvest.workflow.sla_breached{workflow, queue}` once and keeps
running. See "Soft SLA" and "SLA vs `execution_timeout`" in
[`07-reliability-knobs.md`](07-reliability-knobs.md) for the full semantics
(clamping, pause interaction, the fleet-wide
`HarvestBuilder::max_workflow_execution_timeout(…)` ceiling) — they apply to a
DAG's declared values identically. In particular: if `sla` is declared larger
than `execution_timeout`, it is **clamped down** to `execution_timeout` at
start (the hard timeout would kill the run before the soft signal could ever
fire), and the builder-wide ceiling caps a DAG's declared
`execution_timeout` exactly as it caps a plain workflow's.

A DAG declaring neither attribute behaves exactly as before — `null`
`deadline_at`/`sla_deadline_at`, no scanner interaction, zero regression.
`GET /admin/schedules` surfaces the schedule's *effective* deadlines
(`execution_timeout_secs`/`sla_secs`, already clamped) resolved from the
registered workflow or the DAG's shadow `WorkflowInfo`, so an operator can see
what a scheduled DAG's next fire will get without cross-referencing source.

(Classic, non-unified DAGs — `workflow_handler: None` — are already rejected
at plugin startup and are being retired; `execution_timeout`/`sla` are a
unified-DAG-only feature by construction, since the shadow `WorkflowInfo`
that carries them only exists for unified DAGs.)

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

## Passing data between nodes (node input binding)

By default, every DAG node's activity is fed the DAG's *trigger input*
(wrapped in a `{ "conf": …, "dag_task": "<name>" }` envelope) — **not** the
output of the upstream node it depends on. So a classic
`extract → transform → load` pipeline, where each stage consumes the prior
stage's output, previously forced you to either flatten the whole pipeline
into one mega-activity or hand-thread outputs through shared state. Neither
is the graph you wanted to draw.

**Node input binding** (issue #702) closes that gap: bind a node's activity
input directly to one or more upstream node outputs, with one builder call
per data edge and zero hand-written output-threading.

```rust
#[activity(start_to_close = "30s")]
async fn extract_rows(_ctx: &ActivityContext) -> HarvestResult<Value> {
    Ok(serde_json::json!([{ "id": 1 }, { "id": 2 }]))
}

#[activity(start_to_close = "30s")]
async fn transform_rows(_ctx: &ActivityContext, rows: Value) -> HarvestResult<Value> {
    // `rows` IS the extract output, verbatim — no envelope to unpack.
    Ok(serde_json::json!({ "record_count": rows.as_array().map_or(0, Vec::len) }))
}

#[activity(start_to_close = "30s")]
async fn load_summary(_ctx: &ActivityContext, summary: Value) -> HarvestResult<Value> {
    // `summary` IS the transform output, verbatim.
    Ok(Value::Null)
}

#[dag]
pub fn etl_pipeline(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_rows);
    let transform = dag.activity(transform_rows).input_from(&extract);
    let _load = dag.activity(load_summary).input_from(&transform);
}
```

### The three binding methods

| Method | The bound node's activity receives … |
|---|---|
| `.input_from(&up)` | `up`'s recorded output, **verbatim** — no `conf`/`dag_task` wrapper. |
| `.input_from_all(&[&a, &b])` | a JSON object merging both outputs, keyed by each upstream's **activity name**. |
| `.input_from_aliased(&[("k", &a)])` | a JSON object merging the outputs, keyed by the **given alias**. |

`.input_from_all` keys by activity name, so two upstreams sharing an activity
name is a `DagBuildError::DuplicateInputBindingKey` at build time — use
`.input_from_aliased` to disambiguate. A repeated alias is likewise a
`DuplicateInputBindingKey`, and declaring both an `.input_from(…)` binding
and a mapped upstream (`.map_activity(…).over(…)`) on the same node is a
`DagBuildError::ConflictingInputBinding`. All three are caught when the DAG
is compiled, never at run time.

```rust
// Fan-in: merge two upstream outputs into one keyed object.
let users = dag.activity(fetch_users);
let orders = dag.activity(fetch_orders);
let _report = dag
    .activity(join_report)
    .input_from_aliased(&[("users", &users), ("orders", &orders)]);
// join_report receives { "users": <fetch_users out>, "orders": <fetch_orders out> }
```

### Binding implies the dependency edge

A binding **is** a data edge, so it adds the upstream dependency
automatically — you do not also need `.upstream(&up)` (an extra one is
harmless). The bound node therefore runs *after* its bound upstream(s), and
only after their outputs are recorded.

### Skipped or failed upstreams yield null

If a bound upstream was skipped (by a trigger rule or a `.condition`) or
failed, its contribution to the binding is a deterministic `Value::Null` —
never a missing key. By default a node whose upstream was skipped is itself
skipped; to make a bound node run *anyway* and branch on the null, give it a
permissive trigger rule:

```rust
let maybe = dag.activity(maybe_step).upstream(&root).condition(|_| false); // skipped
let _final = dag
    .activity(finalize)
    .input_from(&maybe)               // maybe was skipped → input is null
    .trigger_rule(TriggerRule::AllDone);
```

### Determinism rule

A bound node's input is a **pure function of already-recorded upstream
outputs**. Those outputs are frozen in `harvest_events` before the bound
node is dispatched, so replay reconstructs byte-identical inputs on every
worker and every pass. Do not derive a node's input from process state, the
system clock, or random values — bind to an upstream output instead (and if
you need a captured wall-clock or random value, produce it with
[`ctx.side_effect`](07-reliability-knobs.md) in an upstream activity and read
it back through that activity's output). **No new `WorkflowEvent` variant
and no migration:** binding is computed in the workflow body from recorded
`ActivityCompleted` outputs, and an unbound DAG is byte-identical to today.

### Composition

Input binding composes with the rest of the DAG surface. It slots in
alongside a `.condition(…)` branch (evaluate the branch predicate over
upstream outputs, then bind the chosen node's input), and a bound node's
own output can feed a downstream [mapped fan-out](#dynamic-task-mapping-fan-out)
(`.map_activity(…).over(&bound_node)`) exactly as any other node's output
does. A worked end-to-end example — a three-stage ETL plus a fan-in merge —
lives in `autumn-harvest/examples/dag_data_flow.rs`.

A few interactions worth knowing:

* **A binding is also a `.condition(…)` edge.** Because a binding adds its
  upstream to the node's dependency list, that upstream *also* shows up in the
  node's `.condition(|ups| …)` slice — and `ups` is ordered by the sequence in
  which the builder calls run, not by which method added the edge. So a
  condition that indexes `ups[0]` must account for every `.input_from*` call:
  interleave `.input_from(…)` and `.upstream(…)` deliberately, since their call
  order determines the `ups[…]` indices your predicate sees (`.upstream(&a)`
  before `.input_from(&b)` → `ups[0]` is `a`; the reverse → `ups[0]` is `b`).
* **You can bind to a fan-out node's output.** `.input_from(&mapped_node)` — the
  reverse of "a bound node's output feeds a fan-out" above — is legal; the bound
  node receives the whole *collected array* the mapped node produced, as a single
  JSON array value.
* **An activity used in both bound and unbound positions gets different inputs.**
  The same `#[activity]` reused in a bound node (raw upstream output) and an
  unbound node (the trigger-input + `{ "conf": …, "dag_task": … }` wrapper)
  receives structurally different inputs in each position — write the activity's
  input deserialization to handle both shapes if you reuse it that way.
* **`input_from*` on a signal-gate node is a build error.** A binding on a gate
  is rejected at build time (`InputBindingOnGate`), mirroring the
  `input_from` + `map_activity` conflict. A gate dispatches no activity, so the
  binding *value* is ignored — but unlike the inert activity-only setters
  (`.queue()`, `.retry()`, `.start_to_close()`), a binding also auto-adds a
  dependency edge, which would silently make the gate wait for that upstream
  before its signal wait. Because that edge is a *structural* effect (not an
  inert dead field), the binding is rejected rather than swallowed. Use
  `.upstream(&gate_dependency)` to add a gate dependency deliberately.

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

### Edge traps

- **A JSON `null` payload looks like a timeout.** Under a `Continue` gate the
  timed-out output is `Value::Null`, so a downstream `.condition(|ups|
  ups[0].is_null())` cannot distinguish a timeout from an *approval whose signal
  body was literally `null`*. If your signal payload can legitimately be `null`,
  branch on a field instead (e.g. `.condition(|ups| ups[0].get("approved") ==
  Some(&serde_json::json!(true)))`), not on `.is_null()`.
- **A `Continue` gate cannot feed `.map` directly.** The null timeout output is
  not a JSON array, so `.map_activity(f).over(&gate)` fails at runtime with
  `mapped upstream output is not a JSON array`. Guard the map behind a
  `.condition(|ups| ups[0].is_array())` (or only map over gates whose signal
  payload is always an array — an unbounded `signal_gate` or a `FailRun` gate,
  which never emit the null sentinel).
- **Independent gates in one level are serialized, not concurrent.** Level
  isolation splits every gate into its own singleton execution level, so two
  gates that Kahn-levelling would place together run *sequentially* (the first
  gate resolves, then the second is reached) — they are **not** two overlapping
  wait windows. Gate nodes do not model concurrent signal waits.
- **Only `.upstream()` / `.condition()` / `.trigger_rule()` affect a gate.** A
  gate dispatches no activity, so the activity-only chained setters
  `.retry(...)`, `.start_to_close(...)`, `.queue(...)`, and
  `.map_failure_policy(...)` are accepted by the fluent builder but **silently
  ignored** on a gate node.

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
