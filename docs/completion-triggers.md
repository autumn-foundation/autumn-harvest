# Declarative Completion Triggers

Declarative completion triggers start a target workflow automatically when a source workflow reaches a matching terminal state. This decouples downstream workflow start logic from upstream workflow code, allowing you to orchestrate workflows dynamically without hardcoding dependencies inside the workflow handlers themselves.

---

## Architecture & Semantics

When a workflow execution finishes in a terminal state:
1. **Same-Shard In-Transaction Starts**: If the target workflow's ID routes to the same database shard as the source workflow (or if the source shard is unencoded/default), the target execution is inserted within the same database transaction that persists the source workflow's completion, cancellation, timeout, or failure.
2. **Cross-Shard Out-of-Band Starts**: If the target routes to a different shard, the engine handles the start out-of-band by spawning an asynchronous Tokio task that obtains a database connection from `GLOBAL_SHARDED_POOL` for the target shard and inserts the target execution.
3. **Idempotency & Deduplication**: To prevent duplicate executions in the event of retried transactions or worker crashes, the engine writes to the `harvest_completion_trigger_fires` ledger table on the source shard under a unique constraint: `(source_exec_id, trigger_id)`. A trigger fires exactly once per source execution run.

---

## Database Schema

Triggers use two tables:

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

let harvest = HarvestBuilder::new(pool)
    .completion_trigger(trigger)
    .build();
```

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
| `harvest.completion_trigger.fires` | Counter | `trigger`, `outcome` | Emitted when a completion trigger fires. `outcome` can be: `started` (target execution successfully created), `skipped` (evaluation skipped or duplicate ID already exists but not matching current policy), or `deduped` (idempotency ledger hit). |
