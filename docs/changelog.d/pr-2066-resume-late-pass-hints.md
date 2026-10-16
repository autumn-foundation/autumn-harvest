## Phase 2066 — Republish tasks shifted by the late resume pass (issue #2066)

`resume_queue` shifts held tasks in two passes. The late pass shifts tasks that
the first pass cannot see, such as a task created during the pause. The late
pass returned only a count, so the resume published dispatch hints for first-pass
tasks only.

With a `TaskDispatch` worker, the reference of a late task can sit in release
backoff. The task then waited for the backoff or the reconcile sweep.

The late pass now ends with `RETURNING id`. `resume_queue` adds those ids to the
hint set. It publishes the hints after commit, as it does for the first pass.
The released count is unchanged.

Tests:

- `the_late_resume_pass_returns_the_ids_it_shifts` pins the SQL shape.
- `resume_republishes_hints_for_tasks_created_during_the_pause` drops the
  channel reference of a task created during the pause. It asserts that the
  resume republishes the task.

No new `WorkflowEvent` variant, no new migration, no replay impact.
