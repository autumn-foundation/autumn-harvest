## Fix — cap the interval schedule period (issue #2048)

`validate_schedule` accepted any non-zero whole-second `Schedule::Interval`.
The scheduler adds the period to a timestamp to find the next run. chrono
panics when the sum passes year 262143. A period of about 8.2e12 s or more
made `next_run_after` panic. The management API accepted
`interval:9000000000000000`, so a registration or a scheduler tick could
panic.

`validate_schedule` now rejects a period over `policy::MAX_INTERVAL_PERIOD`
(100 years of 365 days). The error starts with "interval schedule period must
not exceed" and names a manual schedule as the remedy.
`HarvestBuilder::try_build` returns it as
`HarvestBuilderError::InvalidWorkflowSchedule`, for a workflow schedule and
for a DAG schedule. The runtime registration and update functions return
`HarvestError::Config`. The management API returns 400.

Upgrade: an app that registers an interval over 100 years now fails at
build, and `HarvestBuilder::build` panics with the error. Use
`Schedule::Manual` for a schedule that must never fire. Use a period of at
most `MAX_INTERVAL_PERIOD` for a far-future schedule. Before this change,
such a period panicked or never fired in practice.

A row stored before this change can keep an interval over the cap. The
scheduler reads it without validation. Pause, resume and delete still work.
An update that leaves `schedule_expr` unset returns `HarvestError::Config`
(the management API returns 400). Send a period within the cap, or `manual`.

Design decisions:

- Reject at validation, as issue #1967 did for a fractional period. The
  caller gets a typed error at registration, not a panic at the first tick.
- 100 years matches the value that `throttle::compute_expiry` uses for an
  absurd duration. It is far above any real cadence and far below the chrono
  limit.
- The runtime does not trust validation alone. A row stored before this
  change, an embedder that skips validation and a reference near the end of
  chrono time can still reach the addition. `next_run_after` now uses
  `checked_add_signed` and returns `None` on overflow, as for a zero
  interval. `plan_backfill_timestamps` stops its walk. Every caller
  terminates.
- The stored form, the API shape and the CLI do not change.

No migration. No new `WorkflowEvent` variant.

Tests: these failed before the fix.
`interval_schedule_over_the_cap_is_rejected`,
`next_run_after_huge_interval_is_none_not_a_panic`,
`next_run_after_interval_at_the_end_of_time_is_none`,
`plan_backfill_timestamps_huge_interval_returns_only_the_first_slot` and
`plan_backfill_timestamps_interval_stops_at_the_end_of_time`.

These tests cover the cap boundary and each path that calls
`validate_schedule`: `interval_schedule_cap_is_100_years`,
`next_run_after_sum_stops_exactly_at_the_last_date`,
`stored_huge_interval_walks_end`,
`harvest_builder_rejects_workflow_schedule_over_the_interval_cap`,
`harvest_builder_rejects_dag_schedule_over_the_interval_cap`,
`invalid_merged_schedule_writes_nothing` and
`parse_schedule_expr_with_tz_interval_over_the_cap_is_rejected`.
