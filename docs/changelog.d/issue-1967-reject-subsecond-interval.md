## Engine — Reject a sub-second interval schedule (issue #1967)

`validate_schedule` accepted any non-zero `Schedule::Interval`. The stored form
is `interval:<whole seconds>`, so the scheduler dropped the fraction. Each tick
reads the stored form back. A 500 ms schedule became `interval:0`, fired once,
and then stopped. A 1.5 s schedule ran every 1 s.

`validate_schedule` now rejects a period that is not a whole number of
seconds. The error is "interval schedule period must be a whole number of
seconds". `HarvestBuilder::try_build` returns it as
`HarvestBuilderError::InvalidWorkflowSchedule`, for a workflow schedule and
for a DAG schedule.

Upgrade: an app that registers a sub-second or fractional interval now fails
at build. Round the period to whole seconds. That schedule did not run at its
period before this change.

Design decisions:

- Reject, do not store the fraction. The scheduler ticks once a second, so it
  cannot keep a shorter cadence. A new stored form would also break a rolling
  deploy. Each tick registers the schedule again, so old and new nodes would
  overwrite each other's value. An older node also cannot read the new form.
- The stored form, the API and the CLI do not change. They already use whole
  seconds.
- The builder's workflow-schedule check now calls `validate_schedule` for an
  interval too. Its zero-interval error is now "interval schedule period must
  be greater than zero", the same as the other paths.

No migration. No new `WorkflowEvent` variant.

Tests: `schedule_expr_round_trips_every_valid_interval` checks the issue's
oracle: each interval that `validate_schedule` accepts reads back unchanged.
It failed before the fix (`1ns` read back as `0ns`). Two more tests,
`subsecond_interval_schedule_rejected` and
`harvest_builder_rejects_subsecond_workflow_schedule`, also failed before the
fix.
