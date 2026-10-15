## Fix — cap the interval schedule period (issue #2048)

`validate_schedule` accepted any whole-second `Schedule::Interval`. The
scheduler adds the period to a timestamp to find the next run. chrono panics
when the sum passes year 262143. A period of about 8.3e12 s or more made
`next_run_after` panic. The API check `interval:9000000000000000` passed, so
a registration or a scheduler tick could crash the process.

`validate_schedule` now rejects a period over `policy::MAX_INTERVAL_PERIOD`
(100 years of 365 days). The error starts with "interval schedule period must
not exceed". `HarvestBuilder::try_build` returns it as
`HarvestBuilderError::InvalidWorkflowSchedule`. The runtime registration and
update functions return `HarvestError::Config`. The management API returns
400.

Upgrade: an app that registers an interval over 100 years now fails at
build. Use `Schedule::Manual` for a schedule that must not fire. That
schedule did not run at its period before this change.

Design decisions:

- Reject at validation, as issue #1967 did for a fractional period. The
  caller gets a typed error at registration, not a crash at the first tick.
- 100 years matches the cap that `throttle::compute_expiry` already uses for
  an absurd duration. It is far above any real cadence.
- The runtime does not trust validation alone. A row stored before this
  change, an embedder that skips validation and a reference near the end of
  chrono time can still reach the addition. `next_run_after` and
  `plan_backfill_timestamps` now use `checked_add_signed`. An overflow means
  "no next run", as for a zero interval, so every caller terminates.
- The stored form, the API shape and the CLI do not change.

No migration. No new `WorkflowEvent` variant.

Tests: all failed before the fix. `interval_schedule_over_the_cap_is_rejected`,
`next_run_after_huge_interval_is_none_not_a_panic`,
`next_run_after_interval_at_the_end_of_time_is_none`,
`plan_backfill_timestamps_huge_interval_returns_only_the_first_slot`,
`plan_backfill_timestamps_interval_stops_at_the_end_of_time` and
`parse_schedule_expr_with_tz_interval_over_the_cap_is_rejected`.
