## Phase — Keyed entity: serialized handlers over durable state for each key (issue #1975)

`autumn_harvest::entity` adds a keyed entity, like a Restate virtual object
or an Azure durable entity. [ADR 0006](../adr/0006-keyed-entity.md) records
the design and the priority call: P1 for this library layer, rank 13 for a
native primitive.

**Shape.** A `#[workflow]` calls `Entity::new(ctx, checkpoint).run(handler)`.
The key is the `workflow_id`. An operation is an `EntityMessage` on the
`harvest.entity.op` signal, sent with signal-with-start (the generated
`<Name>Stub::signal_with_start`, or the HTTP route). The queries
`harvest.entity.state` and `harvest.entity.status` read the committed state
and the counters.

**Guarantees.**

- One handler at a time for each key. One active run exists for each
  `(workflow_name, workflow_id)`, and the loop awaits each handler.
- State survives a worker crash. Replay rebuilds it.
- An operation is atomic. A handler gets a copy of the state, and only `Ok`
  replaces it. An `Err` or an undecodable message counts as failed, and the
  entity goes on.
- No operation is lost at a checkpoint. The loop drains waiting op signals
  into the continue-as-new input.
- A checkpoint is replay-stable. The loop records each
  `should_continue_as_new()` answer as a side effect. A hand-written loop
  over that call can diverge on replay, because it reads the loaded history
  size. `max_ops_per_run(n)` adds a fixed bound with no event.
- A delete completes the run only when no operation waits.

**Invariants.** No new `WorkflowEvent` variant, no migration and no new
route. Each operation adds one `SideEffectRecorded` event.

**Tests.**

- `entity::tests` (13): serialization, rollback, decode errors, delete,
  checkpoint carry, recorded-decision replay, replay stability against a
  naive loop, and queries.
- `tests/integration/entity_tests.rs` (Postgres):
  `entity_state_survives_a_worker_crash_in_the_middle_of_an_op` and
  `entity_ops_survive_history_checkpoints`.
- `examples/agent_session_entity.rs`: an agent session entity with its own
  tests.
