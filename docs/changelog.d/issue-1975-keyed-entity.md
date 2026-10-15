## Phase — Keyed entity: serialized handlers over durable state for each key (issue #1975)

`autumn_harvest::entity` adds a keyed entity, like a Restate virtual object
or an Azure durable entity. [ADR 0006](../adr/0006-keyed-entity.md) records
the design and the priority call: P1 for this library layer, rank 13 for a
native primitive.

**Shape.** A root `#[workflow]` calls
`Entity::new(ctx, checkpoint).run(handler)`. The key is the `workflow_id`. An
operation is an `EntityMessage` on the `harvest.entity.op` signal, sent with
signal-with-start (the generated `<Name>Stub::signal_with_start`, or the HTTP
route). The queries `harvest.entity.state` and `harvest.entity.stats` read
the committed state and the counters. A start input of `{}` or `null` starts
a new entity.

**Guarantees.**

- One handler at a time for each key. One active run exists for each
  `(workflow_name, workflow_id)`, and the loop awaits each handler.
- State survives a worker crash. Replay rebuilds it.
- State changes are atomic. A handler gets a copy of the state (`S::clone`,
  so use plain data), and only `Ok` replaces it. An `Err` or an
  undecodable message counts as failed, and the entity goes on.
- No operation is lost at a checkpoint. The loop carries waiting op signals
  in the continue-as-new input while it fits the workflow input cap. An op
  that does not fit runs first.
- A checkpoint is replay-stable. The loop records each decision as a side
  effect. A hand-written loop over `should_continue_as_new()` can diverge on
  replay, because it reads the loaded history size. The decision also counts
  this run's own ops, so a backlog in one task still checkpoints. A byte
  estimate (loaded history plus op bytes) trips at half the history byte
  cap. `max_ops_per_run(n)` adds a fixed bound with no event.
- A checkpoint input fits the live cap (issue #2049). The loop records the
  input byte budget at each checkpoint attempt. A cap that a deploy lowers
  while a handler waits on an activity then applies to the next attempt.
- The error text is bounded (issue #2049). `stats.last_error` keeps at most
  1 KiB, cut at a `char` boundary, and ends with `... [truncated]`. A huge
  error cannot stop a checkpoint. A decode error that quotes a payload keeps
  at most 1 KiB of it.
- A delete completes the run only when no operation waits in history.

**Limits.** See the ADR consequences: the delete race, the state size cap,
root workflows only, replay-bound settings, no `execution_timeout`, and a
trusted start input.

**Invariants.** No new `WorkflowEvent` variant, no migration and no new
route. Each operation adds one `SideEffectRecorded` event, or two with an
`execution_timeout`. Each checkpoint attempt adds one more, for the budget.
Crate-private `WorkflowContext` accessors feed the checkpoint decision.

**Tests.**

- `entity::tests`: serialization, rollback, decode errors, delete,
  checkpoint carry, the input-cap budget and its offload rule, the budget
  record at each attempt and its replay under a lowered cap, the
  `last_error` bound, the byte trigger, recorded-decision replay, replay
  stability against a naive loop, the deadline probe, cancellation, bad
  input and queries.
- `tests/integration/entity_tests.rs` (Postgres): a worker crash in the
  middle of an op, with a later op held back; two clients racing on a new
  key; thirty ops across live history checkpoints, with a duplicate key.
- `examples/agent_session_entity.rs`: an agent session entity with its own
  tests, now run in CI.
