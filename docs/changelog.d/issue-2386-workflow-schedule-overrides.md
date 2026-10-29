## Scheduler — Runtime overrides for code-declared workflow schedules (issue #2386, epic #2384)

`PATCH /admin/schedules/{id}` edited a schedule row in place (issue #771). For
a schedule declared with `WorkerBuilder::workflow_schedule`, the next scheduler
tick converged the row back to its code value. The edit lasted about one tick,
and the API still returned 200.

`PATCH /admin/schedules/{id}` on a code-declared schedule now writes an
operator override instead. The override wins over code, survives a restart and
applies on every tick. Each field that the request sets becomes part of the
override. The new `clear_override: true` returns the schedule to its code
value, and the new `reason` is stored with the override. A schedule made with
`POST /admin/schedules/workflow` keeps the #771 row edit and gets no override.

The API tells the two apart with the code catalog of its process, the same
test that `DELETE /admin/schedules` uses.

See [`docs/operations/schedule-overrides.md`](../operations/schedule-overrides.md).

**Behaviour change.** On a code-declared schedule, `input` now returns 400.
Before, it returned 200 and the next tick reverted it. `clear_override` and
`reason` return 400 on a schedule made through the API.

**Migration.** `20261029023935_harvest_schedule_overrides_workflow` lets
`harvest_schedule_overrides.kind` hold `workflow`, and adds the column
`workflow_fields JSONB NOT NULL DEFAULT '{}'`. No other table changes, no
`WorkflowEvent` variant, no replay impact. The grants of #2385 cover it.

**Tests.** `workflow_schedule_override_db_tests` (the patched cadence holds for
three ticks, the override survives new code, clear restores code, a second
patch merges, an invalid patch or a live fire claim writes nothing, an
unreadable override skips the pass) and the `schedule_update_integration`
override tests.
