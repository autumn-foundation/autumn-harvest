## SQLite — Runtime overrides of schedules (issue #2390, epic #2384)

The SQLite backend could not override a schedule at runtime. An app that
upserts its schedules on every open reset each one to its code value.

`SqliteRuntime` gains `set_schedule_override`, `clear_schedule_override`
and `schedule_override`. An override is stored in the new
`harvest_schedule_overrides` table. It wins over the code value of
`upsert_schedule` and survives a reopen with new code, as on Postgres
(#2386). Both backends merge a `WorkflowSchedulePatch` with the same rules.
That type now lives in `autumn_harvest::schedule_patch`, which builds without
the `db` feature. `autumn_harvest::scheduler::WorkflowSchedulePatch` still
names it.

The backend can override the cadence, `max_active_runs`, and the `skip` and
`allow_all` overlap policies. Any other field returns
`SqliteError::Unsupported` with the field name, and writes nothing.
`ScheduleSummary` gains `max_active_runs`.

SQLite has no DAGs, so the DAG override of #2385 has no SQLite form.

**Schema.** `harvest_schedules.code_json` (nullable, added on open) and the
`harvest_schedule_overrides` table. A row from before this change gets its
code value on its next upsert.

**Tests.** `schedule_override_fixture.rs` holds four cases: an override wins
over code, it survives a restart with new code, a clear restores the code
value, and a refused override writes nothing. `schedule_overrides` runs them
on SQLite, and `schedule_override_parity_tests` runs them on Postgres. The
SQLite suite also checks that each unsupported field is refused by name.

See [`docs/operations/schedule-overrides.md`](../operations/schedule-overrides.md#sqlite).
