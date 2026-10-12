## Feature — SQLite backend: workflow updates (issue #2025)

`SqliteRuntime::update(exec, name, args)` runs a workflow update and returns
its result. Before this change, a `RecordUpdateResult` command sealed the run
`FAILED` as `UnsupportedFeature`.

- `update` replays the run into a new context with the core query driver, so
  the body registers its handlers. `register_update` adds declarative
  `#[update]` handlers for a workflow type.
- The arg schema and the validator run before any write. A rejection returns
  `SqliteError::UpdateRejected` and writes no event.
- The handler runs to completion. One transaction appends `UpdateAdmitted`
  and `UpdateCompleted` or `UpdateFailed`. A crash leaves no admitted update
  without a result.
- A `RecordUpdateResult` from the workflow body records the same events, on
  the suspend path and before a terminal event, as on Postgres.
- The history caps (#2023) count the two update events. An update that
  would reach a cap writes no update event, and the run fails at the cap, as
  a drive fails it.
- New errors: `UpdateHandlerNotFound`, `UpdateInputInvalid`,
  `UpdateRejected`, `UpdateFailed`, `UpdateTimedOut` and
  `UpdateHistoryCapReached`. `SqliteError` gains six variants, so an
  exhaustive `match` on it needs new arms.
- A handler can await any future within a 5 second limit. A handler that
  awaits a durable command times out and writes no event.
- The handler runs on a separate replay, so state that it writes does not
  reach the workflow body.
- A replay that drifts from history returns `NonDeterministic` and writes no
  event. A validator panic is a rejection.

No new `WorkflowEvent` variant. No schema change. Tests:
`autumn-harvest-sqlite/tests/integration/updates.rs` and the unit
tests in `autumn-harvest-sqlite/src/update.rs`. The tests replay accepted,
failed, declarative and body-driven histories on the core `WorkflowReplayer`.
A parity test matches the events of `WorkflowTestEnv`. Design:
`DESIGN-2025.md`.
