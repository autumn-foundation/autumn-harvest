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
| Read state | The query `harvest.entity.state` |
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

- One active run exists for each `(workflow_name, workflow_id)`. Signal-with-
  start attaches to that run, so each key has one handler loop.
- The loop takes one op, awaits its handler to the end, then takes the next
  op. Two handlers of one key never overlap, also not across an `await`.
- Ops run in the order of their `SignalReceived` events.

### 4. How the entity survives continue-as-new and history caps

- After each op, the loop asks for a checkpoint. It records the answer of
  `should_continue_as_new()` with `ctx.side_effect`. Replay reads the
  recorded answer, so it takes the checkpoint at the same op. The cost is one
  `SideEffectRecorded` event for each op.
- `max_ops_per_run` adds a fixed bound. It needs no event.
- Before the checkpoint, the loop drains the op signals that wait in history.
  It carries them in the checkpoint input and runs them first in the next
  run.
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
- An op that races the delete of an idle entity can be lost. That is true
  for a signal to any workflow that completes.
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
