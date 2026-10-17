# ADR 0006: Keyed entity as sugar over a workflow

## Status

Accepted (issue #1975). The plan is
[`2026-10-08-keyed-entity.md`](../plans/2026-10-08-keyed-entity.md). The
follow-up plan for the checkpoint budget and the error bound (issue #2049) is
[`2026-10-15-entity-checkpoint-hardening.md`](../plans/2026-10-15-entity-checkpoint-hardening.md).

## Context

- Restate virtual objects and Azure durable entities give each key
  serialized handlers and durable state. Agent sessions use them.
- Harvest had no such primitive. A user combined a deterministic workflow id,
  signal-with-start, a handler loop and queries by hand.
- That recipe has two hidden engine hazards:
  - `continue_as_new` moves only signals that the run did not ingest. A
    signal already in history, but not yet taken by the body, stays on the
    sealed run and is lost.
  - `should_continue_as_new()` reads the size of the loaded history. That
    size grows from task to task. In a loop, a replay can answer `true` at an
    earlier iteration than the live run did, and the replay then diverges.
- Issue #1964 ranks keyed state P1. Issue #1965 ranks it 13, behind customer
  pull.

## Decision

### 1. Sugar, not a new primitive

`autumn_harvest::entity` builds an entity from parts that exist today. It
adds no `WorkflowEvent` variant, no migration and no worker path.

| Entity concept | Harvest part |
|---|---|
| Entity type | A `#[workflow]` function |
| Entity key | The `workflow_id` |
| Operation | A signal named `harvest.entity.op` |
| Send an operation | `signal_with_start` (stub, HTTP or core function) |
| Read state | The queries `harvest.entity.state` and `harvest.entity.stats` |
| Durable state | The workflow input, rebuilt by replay |

### 2. How state is stored and replayed

- The state lives in memory in the body. Replay rebuilds it from the
  checkpoint input, the recorded op signals and the recorded results of the
  handler activities.
- A handler gets a copy of the state (`S::clone`). Only an `Ok` result
  replaces it. The state type must be plain data. A clone that shares
  interior mutability lets a failed handler change the committed state. An
  `Err` or an op that does not decode counts as failed, and the entity goes
  on.
- Queries read the last committed state, never a state inside a handler.

### 3. How handlers are serialized per key

- One active run exists for each `(workflow_name, workflow_id)`.
  Signal-with-start attaches to that run, so each key has one handler loop.
- The loop takes one op, awaits its handler to the end, then takes the next
  op. Two handlers of one key never overlap. This is also true across an
  `await`.
- Ops run in the order of their `SignalReceived` events.

### 4. How the entity survives continue-as-new and history caps

- After each op, the loop asks for a checkpoint. The live answer is
  `should_continue_as_new()`, this run's own op count, or a byte estimate.
  The loaded history count does not include the current task, so a long
  backlog needs the own count. The byte estimate is the loaded history
  bytes plus this run's op bytes. It trips at half the history byte cap, so
  large activity results also force a checkpoint. The loop records the answer with `ctx.side_effect`.
  Replay reads the recorded answer, so it takes the checkpoint at the same
  op.
- The cost is one `SideEffectRecorded` event for each op. A run with an
  `execution_timeout` also records a deadline probe for each op. Each
  checkpoint attempt also records its byte budget.
- The budget is the workflow input cap, or no limit when payload offload
  lifts it. The loop records it before it sizes the input. A deploy can lower
  the cap while a handler waits on an activity. The next attempt then uses the
  lowered cap. Replay of an earlier attempt reads the budget it recorded.
- `max_ops_per_run` adds a fixed bound. It needs no event.
- At a checkpoint, the loop claims the op signals that wait in history. It
  carries them in the checkpoint input while the input fits the workflow
  input cap. An op that does not fit runs in this run, and the loop tries
  again after it. The next run takes the carried ops first.
- A delete op ends the run only when no op waits. Otherwise it resets the
  state to its default and goes on.

### 5. Priority

P1 for this sugar layer, because it is small and agent sessions need it.
A native primitive stays at rank 13, behind customer pull.

## Consequences

- A user writes one `#[workflow]` that calls `Entity::run`. Replay, sharding,
  quotas, pause, reset and erasure apply with no change.
- A retried send with the same idempotency key applies once. The dedupe key
  is `(workflow_name, workflow_id, key)`, so it holds across runs.
- An op has no reply. A caller reads the state with a query. A
  request/response call is future work.
- An op that arrives during the decision that runs a delete is not lost
  (issue #2079). The engine checks for unconsumed signals before it seals a
  run. If one exists, it discards the decision and runs the task again. The
  loop then sees the new op and goes on. A delete that meets a waiting op
  resets the state and does not end the run.
- The idempotency dedupe holds while the earlier runs are retained. A key
  used before a delete also dedupes after it, so use unique keys.
- The checkpoint input holds the whole state. A state larger than the
  workflow input cap fails the run at a checkpoint. Payload offload
  lifts this limit only when its threshold is at or below the cap. Keep
  state small, or trim it in the handler.
- Continue-as-new works only in a root workflow, so an entity cannot be a
  child workflow.
- These values are part of replay: `max_ops_per_run`, the
  `execution_timeout`, the `EntityCheckpoint` wire form, the error text of a
  failed op (it rides in the checkpoint input), and the bound on that text.
  Change them as a versioned workflow change. A new checkpoint field must
  skip serialization at its default.
- `stats.last_error` keeps at most 1024 bytes, marker included
  (`ENTITY_LAST_ERROR_MAX_BYTES`). `Entity::run` cuts a longer error at a
  `char` boundary and ends it with `... [truncated]`. The cut covers the error
  of a failed op and a kept start input. A huge error then cannot stop a
  later checkpoint. The bound counts UTF-8 bytes, and JSON escapes can grow it
  to about 6 KiB in the checkpoint input. A decode error can quote the
  payload, so the cut also limits that quote.
- The deadline check runs only after an op. An idle entity with an
  `execution_timeout` times out. Do not set one on an entity.
- `Entity::run` drops the input of a start that a caller chose (issue
  #2082). `ctx.info().start_source` tells it: `ContinueAsNew`, `Reset`,
  `Rerun` and `Unknown` keep the input. Every other source starts from
  `S::default()`, or from `Entity::initial_state`. A start route or a
  signal-with-start cannot set the first state, the pending ops or the
  counters. `Entity::trust_start_input` restores the old behavior. A run with
  no recorded source is an entity from before the engine recorded it, so it
  keeps its input.
- Declare the workflow input as `serde_json::Value` and build the loop with
  `Entity::from_input`. It decodes the input only when the run keeps it. A
  typed `EntityCheckpoint<S>` input decodes before `Entity::run`, so a
  malformed client input fails the start.
- The gate has three known limits. A reset or a re-run copies the stored
  input of the source run. An admin who resets or re-runs a run that a client
  started with a forged input keeps that input. Workflow code that passes
  caller data to `continue_as_new_as` for an entity type also keeps that data,
  because workflow code is trusted. An older worker reads a source that it
  does not know as `Unknown`, so it keeps the input.
- Existing runs replay from their recorded start. A run that a caller seeded
  before the upgrade replays from `S::default()` after it. Drain those runs
  first, or call `trust_start_input` on the entity before the upgrade.
- A handler panic fails the run, as in any workflow. The next op then
  starts a new entity from the default state.
- The SQLite backend has no queries. It cannot run an entity. (Issue #2023
  later added `continue_as_new` to it.)

## Alternatives considered

- **New event variants and an entity table.** Rejected: a migration, a change
  to the append-only trigger and SQLite work, for no gain a user asked for.
- **Update handlers with an async lock.** Rejected: the body must call each
  admitted update by id, and the lock is the serialization problem again.
- **State outside history behind `ctx.mutex(key)`.** Rejected: replay cannot
  rebuild the state.
- **One workflow for each op with a concurrency limit of 1.** Rejected: no
  state survives between runs.
