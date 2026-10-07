## Fix — wire the remaining DB suites into CI and track the rest (issue #1799)

49 more DB suites now have `linux` manifest rows: 27 core suites and 22 plugin
suites. Before this change, CI compiled them but never ran them. Four core
rows need `testing`: `replay_canary_tests`, `scheduled_time_tests` and
`scheduler_carryover_tests` are gated on it, and one
`transactional_activity_tests` test is too.

Four suites failed against Docker Postgres. Each failure was in the test, not
in the engine:

- `child_policy_tests`: one test predates the capability-miss release of
  issue #804. The default budget kept the parent running past the 30 s wait.
  The test now sets a budget of 0.
- `scheduled_time_tests`: it was the only suite on Postgres 11, the image
  default. Harvest supports Postgres 12+. It now pins Postgres 16.
- `dag_retry_integration`: two tests reset to event 1, which the reset-point
  check refuses. They now use event 4, the boundary of a `step_c` retry.
- `schedule_update_integration`: its hand-rolled migration bundle had no
  `quota_key` column. It now uses `test_init_sql()` and drops
  `harvest_start_throttle`, so the `to_regclass` path stays under test.

The allowlist drops from 55 to 6 entries, and `ALLOWLIST_MAX_LEN` to 6. Four
entries run elsewhere or by hand by design. Two are debt, tracked in #1959.

New page `docs/testing/ci-db-suite-allowlist.md` lists each entry with an
owner or a "won't wire" reason. A new guard test compares it with
`ALLOWLIST`, so the two cannot drift. A reason that the guard does not list
as "by design" counts as debt and needs an owner.

Tests: every wired suite passed locally against Docker Postgres with
`--test-threads=1`. The four fixed suites: `child_policy_tests` 19/19,
`scheduled_time_tests` 4/4, `dag_retry_integration` 12/12 and
`schedule_update_integration` 15/15. The `ci_run_coverage` and
`migration_hygiene` guards pass.

No migration, no route change, and no `harvest_events` change.
