# Keyed entity (issue #1975)

## Problem

Harvest has no keyed entity. Restate virtual objects and Azure durable
entities give each key serialized handlers and durable state. A Harvest user
must combine a deterministic workflow id, signal-with-start, a handler loop and
queries by hand. Each user then meets the same two engine hazards alone (see
the reverse brainstorm).

## Facts (white hat)

- The active-run identity is the pair `(workflow_name, workflow_id)`. One
  active run exists for each pair.
- `signal_with_start_workflow_execution` starts a run or attaches to the live
  run, and stages the signal in the same transaction (issue #244). An
  `idempotency_key` dedupes a retried signal. The dedupe key is
  `(workflow_name, workflow_id, key)`, so it holds across continue-as-new.
- `#[workflow]` generates `<Name>Stub::signal_with_start`. It is the typed
  client call for an entity, so the entity needs no new client function.
- The HTTP route `POST /workflows/{name}/signal-with-start` gives the same
  call to a non-Rust caller.
- A query by business id replays the body to its first suspension and runs the
  handler on that state:
  `GET /workflows/by-id/{name}/{id}/query/{query}`.
- `continue_as_new` moves only signals that the run did not ingest. A signal
  already in history as `SignalReceived`, but not yet taken by the body, stays
  on the sealed run.
- `should_continue_as_new()` compares the size of the loaded history with the
  threshold. The loaded size grows from task to task. In a loop, a replay can
  answer `true` at an earlier iteration than the live run did.
- A side effect records its value once. Replay returns the recorded value.
- The SQLite backend has no continue-as-new and no queries.

## Brainstorm

1. A new primitive: new event variants, an entity table and a new task kind.
2. Sugar over a workflow: a key is a workflow id, an operation is a signal, and
   one loop in the body runs the handler.
3. Sugar over updates, with an async lock in the body to serialize handlers.
4. State in an external store, with `ctx.mutex(key)` around each operation.
5. One workflow for each operation, with a per-key concurrency limit of 1.

Option 2 is the choice. It needs no event variant, no migration and no new
worker path. Replay, sharding, quotas, pause, reset and erasure apply as they
are. Option 1 costs a migration, a trigger change and SQLite parity work for
no gain that a user asked for. Option 3 returns results, but an update
handler can run beside the body, so the lock is the whole problem again.
Option 4 moves state out of history, so replay cannot rebuild it. Option 5
creates one run for each operation and has no state between runs.

## Reverse brainstorm: how to make it fail

| Way to fail | Guard |
|---|---|
| Two runs for one key run handlers at the same time. | The key is the workflow id. Signal-with-start attaches to the live run. |
| Two handlers of one run interleave across an `await`. | One loop takes one operation and awaits its handler to the end. |
| A replay takes a checkpoint at a different operation than the live run. | Record the checkpoint decision with `ctx.side_effect`. |
| Continue-as-new loses operations already in history. | Drain them before the checkpoint and carry them in the input. |
| A failed handler leaves half-changed state. | The handler gets a copy. Only an `Ok` result replaces the state. |
| A bad operation payload kills the entity. | A decode error counts as a failed operation. The loop goes on. |
| A retried client call applies one operation twice. | Pass an idempotency key to signal-with-start. The dedupe holds across runs. |
| A query sees state from the middle of a handler. | Queries read the last committed state only. |
| A delete drops operations queued behind it. | Delete ends the run only when no operation waits. Else it resets the state and goes on. |
| History grows without a bound. | Check for a checkpoint after each operation. `max_ops_per_run` adds a fixed bound. |

## Six hats

- **White.** See the facts above. Issue #1964 ranks keyed state P1 for agent
  sessions. Issue #1965 ranks it 13 and asks for customer pull.
- **Red.** Users want one call, not a recipe. Maintainers fear a new event
  variant on the append-only log.
- **Black.** Each operation adds a side-effect event. An operation that races
  a delete of an idle entity can be lost, as for any workflow that completes.
  No request/response call exists. SQLite cannot run an entity.
- **Yellow.** No migration and no new event. Crash recovery is plain replay.
  The two engine hazards above are solved once, in the library.
- **Green.** Use the signal idempotency key for exactly-once operations. Carry
  pending operations in the checkpoint. Use the generated stub as the client.
  Offer the same shape to HTTP callers with no new route.
- **Blue.** Write ADR 0006. Build the sugar layer under TDD. Add an agent
  session example and a Postgres crash test. Leave a native primitive and
  request/response calls behind customer pull.

## Priority call

P1 for the sugar layer, because it is small and agent sessions need it now.
A native primitive stays at rank 13, behind customer pull. This takes the
P1 view for what ships and the rank-13 view for what does not.

## Steps

1. RED: unit tests for the wire types and harness tests for serialization,
   rollback, decode errors, delete, checkpoint carry and replay.
2. GREEN: `autumn_harvest::entity`.
3. A Postgres test: a worker stops in the middle of an operation, a new worker
   takes over, and the state holds every operation once.
4. The `agent_session_entity` example with its own tests.
5. Docs: ADR 0006, the Temporal migration guide, the comparison page and a
   changelog fragment.
