## Scheduler — Runtime DAG schedule overrides (issue #2385, epic #2384)

A DAG schedule came only from code. The scheduler converged each
`harvest_schedules` row to the code value on every tick, so an operator could
change a schedule only by a restart or a deploy. `PATCH /dags/{dag_name}`
could only pause.

`PATCH /dags/{dag_name}` now also sets an operator override:
`schedule_expr`, `timezone`, `catchup`, `max_active_runs`, `jitter_secs`,
`overlap_policy` and `buffer_all_max`. Each field is tri-state: absent keeps
the stored override, `null` returns to code, a value sets it.
`clear_override: true` returns every field to code. The override wins over
code, survives a restart and applies to every replica.

The scheduler loads the overrides of each shard once per pass and applies
them to the DAG catalog and to the lowered workflow schedules before
registration. The reconciler converges to the effective value with no other
change. `ensure_dag_schedule` applies the override too, so an API path never
undoes it. A pass that cannot read the overrides skips its shard.

See [`docs/operations/schedule-overrides.md`](../operations/schedule-overrides.md).

**Migration.** `20261028195301_harvest_schedule_overrides` adds the table
`harvest_schedule_overrides`. No existing table changes, no `WorkflowEvent`
variant, no replay impact. Each worker role needs `SELECT`, `INSERT`, `UPDATE`
and `DELETE` on it, and the startup preflight checks them.

**API.** Non-breaking for a request that sends `paused` alone. The request
body now rejects an unknown field and an empty body with 400. `paused` is
optional.

**Tests.** `schedule_override_db_tests` (override wins and survives a rebuilt
catalog, a converged row is not rewritten, clear and per-field revert, invalid
override writes nothing, an unreadable override skips the pass, the API path
agrees with the tick, an unscheduled DAG gets a schedule) and
`harvest_api_patch_dag_sets_and_clears_a_schedule_override`.
