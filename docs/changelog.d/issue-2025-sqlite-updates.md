## SQLite backend — workflow updates (issue #2025)

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
- New errors: `UpdateHandlerNotFound`, `UpdateRejected`, `UpdateFailed` and
  `UpdateBlocked`. A handler that awaits a durable command is blocked and
  writes no event.

No new `WorkflowEvent` variant. No schema change. Tests:
`autumn-harvest-sqlite/tests/integration/updates.rs` (14 tests) and the unit
tests in `autumn-harvest-sqlite/src/update.rs`. Each new history replays on the
core `WorkflowReplayer`. Design: `DESIGN-2025.md`.
