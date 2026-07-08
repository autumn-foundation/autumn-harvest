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

## Conditional Triggers — Output Guards (issue #810)

A trigger can carry an optional **output guard**: a bounded, declarative condition evaluated
**server-side at terminal-commit time** against the source workflow's recorded output JSON.
The trigger fires only when the condition evaluates `true`; when it evaluates `false` the fire
is **skipped without starting the target** — no target execution row, no target-side metrics,
no DLQ exposure for the filtered-out case. A trigger with no condition (`NULL` in storage)
fires exactly as before — full backward compatibility.

The guard is a fixed comparison AST, deliberately **not** an expression/CEL engine. It is a
pure, deterministic function: the same recorded output always produces the same fire/skip
decision on every redelivery. No workflow code runs.

### Builder usage

```rust
use autumn_harvest::completion_trigger::{CompletionTrigger, TriggerCondition};
use serde_json::json;

// Fire the high-value fulfillment flow only for orders over 1000 in the EU:
let trigger = CompletionTrigger::new("order_flow", "high_value_fulfillment")
    .with_condition(TriggerCondition::All(vec![
        TriggerCondition::GreaterThan { path: "amount".into(), value: json!(1000) },
        TriggerCondition::Eq { path: "region".into(), value: json!("EU") },
    ]));
```

### JSON shape (management API)

The condition serializes adjacently tagged (`type` / `data`), mirroring `input_mapping`:

```json
{
  "source_workflow_name": "order_flow",
  "target_workflow_name": "high_value_fulfillment",
  "condition": {
    "type": "All",
    "data": [
      {"type": "GreaterThan", "data": {"path": "amount", "value": 1000}},
      {"type": "Eq", "data": {"path": "region", "value": "EU"}}
    ]
  }
}
```

### Operator set (closed)

Leaf operators take a dotted JSON `path` (the same `project_json_path` machinery
`InputMapping::Projection` uses; the empty path selects the whole output):

| Operator | Meaning |
|----------|---------|
| `Eq` / `NotEq` | Equality / inequality against `value` |
| `GreaterThan` / `GreaterThanOrEq` / `LessThan` / `LessThanOrEq` | Numeric ordering against `value` |
| `In` | Membership in the `values` set |
| `Exists` | Path is present in the output (including an explicit `null`) |
| `IsNull` | Path is present AND the value is exactly `null` |
| `All` / `Any` / `Not` | AND / OR / NOT composition (`All([])` = true, `Any([])` = false) |

Evaluation semantics (all defined results — a guard can never panic or 500 the terminal
commit):

* **Missing path or explicit `null`** ⇒ every comparison operator (`Eq`/`NotEq`/ordering/`In`)
  evaluates `false`. Test absence explicitly with `Not(Exists)` and nullness with `IsNull`.
* **Numeric coercion**: two **integers** compare exactly across the full `i64`/`u64` range
  (mixed signs included) — distinct integers above 2^53 (snowflake-style IDs) never collapse.
  When at least one side is a genuine float, both sides compare as `f64` (so `1 == 1.0`;
  mixed int/float pairs above 2^53 follow `f64` precision). Otherwise comparison is strict
  JSON equality (a number never equals a numeric string, and the coercion never recurses
  into arrays or objects — `Eq { value: {"a": 1} }` against a projected `{"a": 1.0}` is
  strict inequality). The ordering operators are **numeric-only** — non-numeric operands
  yield `false`.
* **`NotEq` vs `Not(Eq)`**: `NotEq` on a missing/null path is `false` (comparisons require
  presence), while `Not(Eq(..))` on the same path is `true` (`Not` is pure negation). Pick
  deliberately — they differ exactly when the path is absent or null.
* A non-`Completed` source usually has a `NULL` recorded output, which evaluates as a literal
  JSON `null` root: member paths are all missing, so comparisons are `false` — but a
  `Not(Exists(..))`-style guard can still meaningfully fire. Guards are output-only by design
  (the failure-cause envelope is issue #748's story).

### Boundedness caps (registration-time validation)

A hostile or accidental mega-condition can never reach the terminal-commit path. Both
registration surfaces — `HarvestBuilder::try_build()` (builder error) and
`POST /admin/completion-triggers` (`400`) — reject:

* nesting depth > **8** (`MAX_CONDITION_DEPTH`)
* total nodes > **64** (`MAX_CONDITION_NODES`)
* `In` sets longer than **64** (`MAX_CONDITION_IN_VALUES`)
* malformed dotted paths (empty segments, e.g. `"a..b"`, `".a"`)
* unknown operators (a serde deserialization error → `400`; never silently dropped). The
  HTTP handler decodes the `condition` field from raw JSON itself rather than letting the
  typed `Json` extractor do it — axum surfaces extractor-level data errors as a `422` with
  a plain-text body, whereas the handler-level decode produces the documented `400` JSON
  error for every invalid-condition shape uniformly.

There is no separate byte-size cap on condition payloads: over HTTP, total condition bytes
are bounded by axum's default request body limit (2 MiB), and evaluation is a linear walk
with no amplification.

### Skips are recorded exactly-once and observable

A condition-skip is **resolved-skipped**, not silently dropped:

* The `(source_exec_id, trigger_id)` row is inserted into
  `harvest_completion_trigger_fires` through the same `ON CONFLICT DO NOTHING` PK path a real
  fire uses, with the skip reason recorded in the additive `outcome` column
  (`NULL` = fired, `condition_unmet` = skipped). A re-delivered terminal (e.g. the
  parent-close cascade re-entering evaluation) dedupes against that row exactly like a real
  fire — a skipped terminal can never late-fire, even if the condition is later loosened.
  Note the row's `fired_at` column records the **resolution** time — the moment the pair was
  durably decided (fire *or* skip) — not necessarily a fire.
* The new counter `harvest.completion_trigger.skipped{trigger, reason}` increments once per
  fresh skip (see Telemetry below). Coverage caveat: on the operator cancel/terminate and
  parent-close-cascade terminal paths the counter is best-effort (no metrics recorder is
  threaded there today), so the fires-row `outcome` column is the **authoritative** skip
  record; the counter is emitted on the worker, timeout-scanner, poison-pill, history-cap,
  and workflow-task-timeout paths.

### Fail-closed on invalid stored conditions

If a stored `condition` no longer parses (or violates the caps — e.g. a row written by a
different build during a mixed-version deploy), the trigger **fails closed**: the fire is
skipped with the distinct reason `condition_invalid`, a warning is logged, and — unlike a
`condition_unmet` skip — **no fires row is written** (mirroring the `validation_failed`
precedent), so the pair is left *unresolved* rather than durably suppressed. Harvest never
fires on an unintelligible guard and never errors the source's terminal commit — a corrupt
trigger condition must never wedge unrelated workflow closes.

**Honest recovery contract.** Trigger evaluation runs only inside a source workflow's
terminal commit; there is no background scanner that re-visits unresolved pairs. A
re-evaluation for an already-terminal execution therefore only happens if some later path
re-enters `evaluate_triggers_for_execution` for that execution (e.g. a parent-close-cascade
race) — which is **not guaranteed**. Without such a re-entry, the fire is **lost for that
terminal**, even after the fleet is upgraded or the condition repaired. What "no fires row"
buys you is that a re-entry, if one happens, *can* still fire (the pair was never durably
resolved) — it is not an automatic retry.

Operational guidance:

* **Detection:** alert on `harvest.completion_trigger.skipped{reason="condition_invalid"}`
  (and the paired `tracing::warn!`, which names the `trigger_id` and `source_exec_id`).
  Each such skip should be treated as a lost fire: start the target workflow by hand (the
  fires-row PK path means a manual start cannot double-fire a later re-entry — the re-entry
  would insert the fires row; hand-started targets use their own workflow ids).
* **Prevention:** the mixed-version window is *new-binary registrations racing old-binary
  workers*. During a rolling deploy, register (or update) conditions that use newly added
  operators only **after** the whole fleet runs the new binary; conditions using only
  operators every deployed build understands are safe to register at any time. See
  "Rollout ordering" below for the full deploy-ordering contract — including the strictly
  worse pre-#810 window, where an old worker does not fail closed at all but fires the
  trigger unconditionally.

A durable re-evaluation queue for `condition_invalid` pairs is a named follow-up (see the
PR #972 review thread); it is deliberately not built here to keep the guard slice
self-contained ahead of issue #748.

### Rollout ordering — guards are enforced by workers, not at registration

A guard is evaluated by whichever **worker binary** commits the source workflow's
terminal transition — never by the API node that accepted the registration. A guarded
trigger is therefore only enforced by workers running a build that understands its
`condition`, and there are two distinct mixed-version windows:

* **Pre-#810 worker binaries do not fail closed — they fire unconditionally.** A worker
  built before this feature selects the trigger row *without* the `condition` column
  (Diesel queries name their columns explicitly, so the extra column is invisible rather
  than an error) and its evaluation path has no gate at all. If the source workflow
  closes on such a worker, the target starts as if the trigger were unguarded — and
  because that binary never sees the guard, it emits **no warning, no skip counter, no
  fires-row `outcome`**. This window cannot be detected from telemetry; it can only be
  avoided by deploy ordering.
* **#810+ binaries that cannot parse a specific stored condition fail closed**
  (`condition_invalid`, previous section) — this is the future-operator window, and it
  *is* detectable (warn + counter).

**Rule: upgrade the entire worker fleet first, then register guarded triggers.** The
same ordering applies to every future condition extension: register conditions that use
newly added operators only after every deployed worker understands them.

Harvest deliberately does **not** reject guarded registrations based on observed fleet
state. `harvest_workers.build_id` is an optional, free-form operator label (often empty)
with no mapping to "understands guard operator X"; heartbeat rows can be stale; and a
pre-#810 worker can join the fleet the moment after any registration-time check passes —
such a gate would be unreliable rather than protective. This matches the rollout
contract of every other additive behavioral column in Harvest (completion callbacks
#605, soft SLA #487, debounce #499, schedule catchup #484): documented deploy ordering,
not registration gating.

### Re-registering a trigger replaces the whole definition

`POST /admin/completion-triggers` (and the builder sync) has full-replacement upsert
semantics: re-POSTing an existing trigger `id` overwrites every field — **omitting
`condition` clears a previously registered guard**, silently reverting the trigger to
unconditional. Always include the current `condition` when updating any other field of a
guarded trigger.

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
    "condition": null,
    "created_at": "2026-06-03T07:00:00Z",
    "updated_at": "2026-06-03T07:00:00Z"
  }
]
```

The `condition` field is always present in responses (`null` = unconditional); see
[Conditional Triggers](#conditional-triggers--output-guards-issue-810) for its shape.

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
  },
  "condition": {
    "type": "GreaterThan",
    "data": {"path": "results.amount", "value": 1000}
  }
}
```

The optional `condition` output guard is validated at registration (`400` on an unknown
operator, over-cap tree, or malformed path — see Conditional Triggers above). Upserts are
full-replacement: omitting `condition` on a re-POST clears any previously stored guard.

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
  "condition": {
    "type": "GreaterThan",
    "data": {"path": "results.amount", "value": 1000}
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
| `harvest.completion_trigger.skipped` | Counter | `trigger`, `reason` | Emitted on output-guard skips (issue #810). `reason` is `condition_unmet` (guard evaluated false; emitted once per fresh skip — a redelivered, already-resolved skip records `deduped` on the fires counter instead) or `condition_invalid` (stored condition unparseable/over-cap — fail-closed, no fires row; the fire is lost for that terminal unless evaluation re-enters, so treat each occurrence as an alert to re-trigger the target by hand — see "Fail-closed on invalid stored conditions"). |

The `outcome` label takes one of:

| `outcome` | Meaning |
|-----------|---------|
| `started` | The target execution was successfully created (new run). |
| `skipped` | The dedup ledger admitted the fire, but start-or-load resolved to an already-existing run rather than creating a new one (`created == false`). |
| `deduped` | The `harvest_completion_trigger_fires` idempotency ledger already had a row for `(source_exec_id, trigger_id)` — a duplicate delivery of an already-resolved fire **or output-guard skip** (issue #810); no target was started. |
| `validation_failed` | The mapped input failed validation against the target workflow's published input schema; the target was not started. |
| `payload_too_large` | The mapped input exceeded the target workflow's max input byte cap; the target was not started (permanent error). |

`started`, `skipped`, and `deduped` are the canonical wiring-confirmation outcomes called out in issue #517; `validation_failed` and `payload_too_large` are additional safety outcomes emitted by the implementation so operators can see triggers that matched but were intentionally dropped.
