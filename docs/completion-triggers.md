# Declarative Completion Triggers

Declarative completion triggers start a target workflow automatically when a source workflow reaches a matching terminal state. This decouples downstream workflow start logic from upstream workflow code, allowing you to orchestrate workflows dynamically without hardcoding dependencies inside the workflow handlers themselves.

---

## Architecture & Semantics

When a workflow execution finishes in a terminal state:
1. **Same-Shard In-Transaction Starts**: If the target workflow's ID routes to the same database shard as the source workflow (or if the source shard is unencoded/default), the target execution is inserted within the same database transaction that persists the source workflow's completion, cancellation, timeout, or failure.
2. **Cross-Shard Out-of-Band Starts**: If the target routes to a different shard, the engine handles the start out-of-band by spawning an asynchronous Tokio task that obtains a database connection from `GLOBAL_SHARDED_POOL` for the target shard and inserts the target execution.
3. **Idempotency & Deduplication**: To prevent duplicate executions in the event of retried transactions or worker crashes, the engine writes to the `harvest_completion_trigger_fires` ledger table on the source shard under a unique constraint: `(source_exec_id, trigger_id)`. A trigger fires exactly once per source execution run.

### No new event variant

Trigger evaluation introduces **no new `WorkflowEvent` variant**. The source emits nothing extra on completion, and the target's start emits its own ordinary `WorkflowStarted`. The append-only event contract is preserved by construction — completion triggers are a registry plus a fire ledger layered *outside* the event log.

### Deterministic target `workflow_id`

The target execution's `workflow_id` is **deterministic** and derived from the trigger and source execution:

```
completion-trigger-{trigger_id}-{source_exec_id}
```

This is what makes duplicate fires collapse safely. The start always goes through `start_or_load_workflow_execution` with `WorkflowIdReusePolicy::AllowDuplicate`, so if the same `(trigger, source execution)` pair is ever evaluated more than once (a re-delivery, a replayed terminal transition, or a cross-shard retry), the second start resolves to the *same* `workflow_id` and is collapsed by start-or-load rather than creating a second run. The `harvest_completion_trigger_fires` ledger is the primary at-most-once guard; the deterministic id is the second line of defense at the start path.

### Cross-shard delivery contract

If the target routes to a **different** shard than the source, the fan-out crosses the shard boundary via the existing start path — **there is no cross-shard transaction**. Instead, within the source-shard terminal-commit transaction the engine inserts a row into `harvest_completion_trigger_outbox` (alongside the fires-ledger insert), and then attempts the target start out-of-band:

* The async `DeferredTriggerStart::spawn` path attempts the start immediately and deletes the outbox row on success.
* The `enforce_completion_triggers_outbox` sweeper (wired into the timeout/enforcement scan loop) drains any outbox rows whose immediate spawn was lost to a crash or a transient connection failure, retrying the start on the target shard.

The contract across the shard boundary is therefore **at-least-once delivery, deduped at the target**: the outbox guarantees the start is *eventually* attempted at least once even if a worker dies between the source commit and the target start, and the deterministic `workflow_id` + start-or-load reuse policy guarantee that repeated attempts collapse into exactly one target run. Same-shard starts are stronger (they share the source's commit transaction), but both boundaries converge on exactly-one target execution after dedupe.

---

## Database Schema

Triggers use three tables:

### `harvest_completion_triggers`
Stores the static completion trigger definitions synced at startup or registered dynamically:
```sql
CREATE TABLE harvest_completion_triggers (
    id UUID PRIMARY KEY,
    source_workflow_name VARCHAR(255) NOT NULL,
    terminal_states JSONB NOT NULL,
    target_workflow_name VARCHAR(255) NOT NULL,
    input_mapping JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### `harvest_completion_trigger_fires`
Acts as the deduplication ledger to guarantee at-most-once triggering semantics:
```sql
CREATE TABLE harvest_completion_trigger_fires (
    source_exec_id UUID NOT NULL,
    trigger_id UUID NOT NULL REFERENCES harvest_completion_triggers(id) ON DELETE CASCADE,
    fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_exec_id, trigger_id)
);
```

### `harvest_completion_trigger_outbox`
Holds cross-shard pending target starts. A row is written on the source shard inside the terminal-commit transaction when the target routes to a different shard, and deleted once the target start succeeds (either by the immediate async spawn or by the `enforce_completion_triggers_outbox` sweeper). It is the durable backstop that makes cross-shard delivery at-least-once:
```sql
CREATE TABLE harvest_completion_trigger_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_exec_id UUID NOT NULL,
    trigger_id UUID NOT NULL,
    target_shard INT NOT NULL,
    target_workflow_name VARCHAR(255) NOT NULL,
    target_workflow_id VARCHAR(255) NOT NULL,
    target_input JSONB NOT NULL,
    queue_name VARCHAR(255),
    concurrency_key VARCHAR(255),
    concurrency_limit INT,
    priority JSONB NOT NULL,
    max_workflow_input_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## Builder API

You can register triggers statically on startup using `HarvestBuilder::completion_trigger(...)` or `HarvestBuilder::completion_triggers(...)`:

```rust
use autumn_harvest::HarvestBuilder;
use autumn_harvest::completion_trigger::{CompletionTrigger, TerminalState, InputMapping};
use serde_json::json;

let trigger = CompletionTrigger::new("upstream-processing", "downstream-reporting")
    .with_terminal_states(vec![TerminalState::Completed])
    .with_input_mapping(InputMapping::Projection("results.summary".to_string()));

let harvest = HarvestBuilder::new()
    .completion_trigger(trigger);
```

---

## Worked example: two-stage ETL → reporting

A daily pipeline where a reporting workflow must run **after** the ingest workflow finishes,
carrying the ingest's summary as its input — without the ingest workflow importing or knowing
about the reporting workflow:

```rust
use autumn_harvest::HarvestBuilder;
use autumn_harvest::completion_trigger::{CompletionTrigger, TerminalState, InputMapping};

// Stage 1: `etl_ingest` runs (on a cron schedule, manually, or however you like)
// and returns an output like { "summary": { "rows": 12000, "date": "2026-06-24" }, ... }.
//
// Stage 2: `daily_report` should run the moment `etl_ingest` COMPLETES, receiving just the
// `summary` sub-object as its input.
let pipeline = CompletionTrigger::new("etl_ingest", "daily_report")
    .with_terminal_states(vec![TerminalState::Completed]) // Completed is also the default
    .with_input_mapping(InputMapping::Projection("summary".to_string()));

let harvest = HarvestBuilder::new()
    .completion_trigger(pipeline);
```

When an `etl_ingest` run with execution id `E` completes, Harvest starts `daily_report` with a
deterministic id `completion-trigger-{trigger_id}-E` and input equal to the ingest output's
`summary` field. If `etl_ingest` instead FAILS, nothing starts (the trigger only matches
`Completed`). Wiring this dependency touched **neither** `etl_ingest`'s nor `daily_report`'s
body — it is a single declarative registration. See
[`examples/completion_triggers.rs`](../autumn-harvest/examples/completion_triggers.rs) for a
runnable version.

---

## Input Mappings

Triggers support projection mapping from the source workflow output payload:
* **`Passthrough`**: Forwards the exact source workflow output payload to the target workflow's input.
* **`Static(Value)`**: Provides a predefined, static JSON value as the target workflow's input.
* **`Projection(String)`**: Projects a value out of the source output using dotted-path syntax (e.g. `results.summary.count`).

---

## Axum Admin APIs

Triggers can be listed or registered dynamically at runtime using the following REST endpoints:

### 1. GET `/admin/completion-triggers`
Returns a list of all registered completion triggers in the deployment (read from the default shard).

**Response (200 OK)**:
```json
[
  {
    "id": "78099354-94b3-4f24-9b16-621be94576ff",
    "source_workflow_name": "upstream-processing",
    "terminal_states": ["Completed"],
    "target_workflow_name": "downstream-reporting",
    "input_mapping": {"type": "Passthrough"},
    "created_at": "2026-06-03T07:00:00Z",
    "updated_at": "2026-06-03T07:00:00Z"
  }
]
```

### 2. POST `/admin/completion-triggers`
Dynamically registers or updates a completion trigger. The engine propagates trigger registrations across all shards.

**`terminal_states`** accepts any of: `"Completed"`, `"Failed"`, `"Cancelled"`, `"TimedOut"`, `"Terminated"`. `"Cancelled"` matches a *cooperative* cancel (`POST /workflows/{id}/cancel`); `"Terminated"` matches a *forceful* operator/batch/scheduler terminate (`POST /workflows/{id}/terminate`, issue #504). They are distinct — a force-terminate does **not** fire a `"Cancelled"` trigger, and a cancel does not fire a `"Terminated"` trigger. Register both if you want a downstream workflow on either outcome.

**Request Body**:
```json
{
  "id": "78099354-94b3-4f24-9b16-621be94576ff",
  "source_workflow_name": "upstream-processing",
  "terminal_states": ["Completed", "Failed"],
  "target_workflow_name": "downstream-reporting",
  "input_mapping": {
    "type": "Projection",
    "data": "results.summary"
  }
}
```

**Response (201 Created)**:
```json
{
  "id": "78099354-94b3-4f24-9b16-621be94576ff",
  "source_workflow_name": "upstream-processing",
  "terminal_states": ["Completed", "Failed"],
  "target_workflow_name": "downstream-reporting",
  "input_mapping": {
    "type": "Projection",
    "data": "results.summary"
  },
  "created_at": "2026-06-03T07:01:00Z",
  "updated_at": "2026-06-03T07:01:00Z"
}
```

---

## Telemetry & Metrics

Completion triggers emit the `harvest.completion_trigger.fires` metric counter to track trigger evaluation outcomes:

| Metric | Instrument | Labels | Description |
|--------|------------|--------|-------------|
| `harvest.completion_trigger.fires` | Counter | `trigger`, `outcome` | Emitted on every completion-trigger evaluation. See the `outcome` values below. |

The `outcome` label takes one of:

| `outcome` | Meaning |
|-----------|---------|
| `started` | The target execution was successfully created (new run). |
| `skipped` | The dedup ledger admitted the fire, but start-or-load resolved to an already-existing run rather than creating a new one (`created == false`). |
| `deduped` | The `harvest_completion_trigger_fires` idempotency ledger already had a row for `(source_exec_id, trigger_id)` — this is a duplicate fire and no target was started. |
| `validation_failed` | The mapped input failed validation against the target workflow's published input schema; the target was not started. |
| `payload_too_large` | The mapped input exceeded the target workflow's max input byte cap; the target was not started (permanent error). |

`started`, `skipped`, and `deduped` are the canonical wiring-confirmation outcomes called out in issue #517; `validation_failed` and `payload_too_large` are additional safety outcomes emitted by the implementation so operators can see triggers that matched but were intentionally dropped.
