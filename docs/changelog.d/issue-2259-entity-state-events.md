## Engine — Keyed-entity state as history events with a Postgres projection (issue #2259)

Stage 3a of epic #2247. [ADR 0006](../adr/0006-keyed-entity.md) records the
amendment. The design record is [`DESIGN-2259.md`](../../DESIGN-2259.md).

**Event.** The entity loop records a state snapshot as
`SideEffectRecorded` with the new kind `SideEffectKind::EntityState`. It
records one at the start of each run, one after each op, and one before a
delete ends the run. The value is `{"state": <S>, "stats": <EntityStats>}`.
Replay adopts each recorded snapshot, so it rebuilds the state from the
events. A tolerant matcher reads a history from before this change as having
no snapshot, so old entity histories replay with no change. Upgrade every
worker before entity runs move to the new release: an older worker cannot
decode the new kind.

**Migration.** `20261023015028_harvest_entity_state` adds the table
`harvest_entity_state`, the function `harvest_entity_state_project` and the
trigger `harvest_entity_state_projection_trg` on `harvest_events`. The
trigger runs `AFTER INSERT` with a `WHEN` clause, and writes the latest
snapshot of each key in the transaction that inserts the event. Every insert
path keeps the projection in step: a decision, a reset, a fork and a shard
move. The append-only guard does not change. The partition layout changes
reinstall the new trigger with the guard.

**Coverage.** `codec_rotation::CODEC_COLUMNS` lists
`harvest_entity_state.snapshot`. Erasure tombstones the row of each erased
run and reports `entity_states_scrubbed`.

**Reads.** `autumn_harvest::entity_state` reads a row by key, reads the row
of a run, and lists the rows of an entity type. The HTTP query routes and
`WorkflowHandle::execute_query_in_process` answer `harvest.entity.state` and
`harvest.entity.stats` from the projection. They replay only when the
queried run wrote no row.

**Tests.** `entity::tests` cover the snapshots, replay from snapshots, old
histories, continued and reset runs and the byte budget.
`entity_state_db_tests` covers a crash at every step, a worker crash, run
ordering, a reset of a continued run, erasure, codec rotation and each
partition layout change. `entity_state_query_integration` proves that the
HTTP query does not run the workflow body.
