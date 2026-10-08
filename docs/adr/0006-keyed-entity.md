# ADR 0006: Keyed entity as sugar over a workflow

## Status

Accepted (issue #1975). The plan is
[`2026-10-08-keyed-entity.md`](../plans/2026-10-08-keyed-entity.md).

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
- A handler gets a copy of the state. Only an `Ok` result replaces it. An
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
  `should_continue_as_new()`, or this run's own op count and op bytes. The
  loaded history count does not include the current task, so a long backlog
  needs the own count. The loop records the answer with `ctx.side_effect`.
  Replay reads the recorded answer, so it takes the checkpoint at the same
  op.
- The cost is one `SideEffectRecorded` event for each op. A run with an
  `execution_timeout` also records a deadline probe for each op. A checkpoint
  records its byte budget once.
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
- An op that arrives during the decision that runs a delete can be lost. The
  run completes, and the engine does not re-check new signals at completion.
  A retry with the same idempotency key then dedupes against the completed
  run. Send a delete only when no client still sends to the key.
- The idempotency dedupe holds while the earlier runs are retained. A key
  used before a delete also dedupes after it, so use unique keys.
- The checkpoint input holds the whole state. A state larger than the
  workflow input cap fails the run at a checkpoint, unless payload offload
  is on. Keep state small, or trim it in the handler.
- Continue-as-new works only in a root workflow, so an entity cannot be a
  child workflow.
- These values are part of replay: `max_ops_per_run`, the
  `execution_timeout`, the `EntityCheckpoint` wire form, and the error text
  of a failed op (it rides in the checkpoint input). Change them as a
  versioned workflow change. A new checkpoint field must skip serialization
  at its default.
- The deadline check runs only after an op. An idle entity with an
  `execution_timeout` times out. Do not set one on an entity.
- The start input is trusted. A caller who may start the workflow may set
  the first state. Clients send `{}` or no input.
- A handler panic fails the run, as in any workflow. The next op then
  starts a new entity from the default state.
- The SQLite backend has no continue-as-new and no queries. It cannot run an
  entity.

## Alternatives considered

- **New event variants and an entity table.** Rejected: a migration, a change
  to the append-only trigger and SQLite work, for no gain a user asked for.
- **Update handlers with an async lock.** Rejected: the body must call each
  admitted update by id, and the lock is the serialization problem again.
- **State outside history behind `ctx.mutex(key)`.** Rejected: replay cannot
  rebuild the state.
- **One workflow for each op with a concurrency limit of 1.** Rejected: no
  state survives between runs.
