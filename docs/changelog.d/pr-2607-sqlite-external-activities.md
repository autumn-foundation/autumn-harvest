## External activities on the SQLite backend (issue #2607)

`ctx.execute_activity_external(...)` now runs on the SQLite backend. Before,
the backend sealed such a run `FAILED` with `UnsupportedFeature`. The design
is in `docs/design/DESIGN-2607.md`.

- The decision cycle records the handoff in one transaction: the
  `ActivityAwaitingExternal` event, a row in the new
  `harvest_external_tasks` table and a deadline timer. A re-park writes
  nothing, so the deadline counts from the first record.
- A drive returns the new `RunState::WaitingExternal(token)`. The fleet
  drivers count it as blocked. **Breaking:** a `match` on `RunState` or
  `SqliteError` with no wildcard arm needs the new arms.
- `SqliteRuntime::complete_external(token, output)` and
  `fail_external(token, error, retryable)` settle a token. As on Postgres, a
  second settle or a settle after the run ends returns `false`. An unknown
  token is `SqliteError::ExternalTaskNotFound`. An output nested past
  `MAX_PAYLOAD_DEPTH` fails the activity instead. A settle on an overdue run
  seals it `TIMED_OUT` first.
- `SqliteRuntime::list_external_handoffs(&ExternalHandoffFilter)` reads the
  handoffs by state, activity name, run or token. Rows come by deadline, as
  Postgres `list_external_handoffs` orders them. Each row carries the input.
- The schedule-to-close deadline is a `harvest_timers` row with the id
  `__external_deadline:<token>`. Its fire records
  `ActivityTimedOut { ScheduleToClose }`, not `TimerFired`.
  `extend_external_deadline` moves it and records
  `ActivityExternalDeadlineExtended`.
- Each terminal setter sets the `PENDING` tokens of its run `CANCELLED`.
  Postgres leaves them `PENDING`. Retention purges the new table with the
  run. `HarvestPlugin` on SQLite does not serve the external routes.
- No new `WorkflowEvent` variant. No Postgres migration. The SQLite table is
  new, so an older file gains it on open.

Tests: `autumn-harvest-sqlite/tests/integration/external_activities.rs`
has 15 integration tests. They cover the park, a complete with an
idempotent second settle, a fail and an unknown token. They also cover
the deadline fire, a settle that removes the deadline, and a re-park. The
rest cover the list order and filters, an ended run, a cancel, the fleet
drivers, an extension, a deep output, a reopen and an overdue run. Three
conformance scenarios compare a completed, a failed and a timed-out
external activity with Postgres. `ScheduleExternalActivity` moves from the
rejected to the accepted command kinds. The SQLite tests that used it as
their stock unsupported command now use a worker session or a cross-type
continue-as-new.
