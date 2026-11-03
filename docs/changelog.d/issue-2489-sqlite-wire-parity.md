## Plugin — SQLite management API keeps the Postgres wire contract (issue #2489, epic #2466)

The SQLite `HarvestPlugin` routes now answer as the Postgres routes do. An
audit of every ported route found each difference. Each one is ported or
documented in `docs/sqlite-plugin.md`. The plan is
`docs/design/DESIGN-2489.md`.

- **`GET /workflows/{id}/result`** and its by-id form. A run that is not
  terminal answers `204` with `Retry-After: 1`. A terminal run answers the
  compact `WorkflowResult`. The read follows the continue-as-new and
  workflow retry chains to the last run. `wait` long-polls.
- **History paging.** `GET /workflows/{id}/history` takes `after`, `limit`
  and `event_type`, with the Postgres cursor. Each event has `id`,
  `event_id`, `timestamp`, `type` and `data`. `SqliteReader::history_page`,
  `SqliteReader::successor` and `SqliteReader::parent` are new.
  `harvest_events` gains a `recorded_at` column. The `INSERT` writes it, so
  the append-only guard is unchanged. Replay does not read it.
- **Reads.** Describe caps its history at 100 events and adds `parent_id`,
  `external_handoffs` and `legal_hold`. A by-id read sets
  `X-Harvest-Execution-Id`, prefers the running run, and refuses an empty
  id with `400`. A listing refuses an unknown `state` and pages 50 / 200.
  `/workflows/registered` returns the full record. `/health/live` and
  `/health/ready` keep the Postgres bodies.
- **Writes.** A signal answers `202`, and `400` for a terminal run. A
  signal-with-start that starts a run answers `201`. A duplicate start
  answers the Postgres `409` body. A repeat cancel or terminate answers an
  idempotent `202`. An update answers `update_id`, and `409` when its
  handler fails. Query errors map to `400`, `408` and `410`.
- **Retry routing (#843).** A signal, cancel, terminate, query or update to
  a `FAILED` run with a live retry acts on the retry. A signal names it in
  `routed_execution_id`.
- **One contract, two backends.** `tests/support/wire_contract.rs` holds the
  checks. The SQLite suite and the Postgres result, history and by-id
  integration tests run them.
