## Phase X.Y — Schedule health end_at check skips jitter when slot is unknown (issue #1568)

The Vantage schedule health check judged `end_at` against the jitter-adjusted
`next_run_at`. The scheduler hashes jitter against a different slot in two
cases. A calendar can rebase the slot first. The `MostRecent` and `Window`
catchup policies can pick a later slot first. The UI then reported live
schedules as exhausted.

`schedule_is_bounded_out` now judges the raw slot for those rows. `SkipAll`,
`Unbounded` and plain rows keep the jitter-adjusted check from #1293. The
catchup policy resolves through `CatchupPolicy::from_db`, as in the scheduler.

Trade-off: a raw slot before `end_at` is not reported as exhausted, even if the
scheduler later stops it. Full accuracy needs calendar exclusions loaded per
page render. That work stays open.

No migration. No `WorkflowEvent` change. `harvest_events` is not touched.

Tests: `end_at_exhaustion_ignores_jitter_when_calendar_is_set`,
`end_at_exhaustion_ignores_jitter_for_slot_selecting_catchup`,
`end_at_exhaustion_keeps_jitter_for_first_slot_catchup`.
