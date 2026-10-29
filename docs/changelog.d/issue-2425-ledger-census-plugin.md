## Tooling — Ledger census batch 5: the plugin test suites (issue #2425)

The Ledger census now runs in the plugin's test suites too. The census hook
moved out of the core test binary into a `ledger-census` cargo feature, so any
test binary can link it. `docs/ledger/README.md` gives the commands.

729 Postgres profiles now link a plan page under `docs/ledger/plans/`, up from
552. Most of the new ones are `per_request` profiles of the admin API, the UI
and the status endpoints, which only the plugin's tests reach.

The census found a correctness bug, fixed here, and four scans worth an index
decision (issue #2424). More dead-letter and signal readers were added to
issues #2401 and #2367.

## Fixed — schedule budget updates touched every schedule (issue #2423)

Trigger-now and backfill reserve a `max_runs` slot with a conditional
`UPDATE`. Its budget guard was a raw SQL fragment with a bare `OR`, and
`AND` binds tighter than `OR`. So the `id` guard covered one branch only, and
the statement also matched every capped schedule under its cap on that shard.

- Trigger-now and backfill reservation incremented `runs_started` on those
  schedules too.
- Backfill release cleared `exhausted_at` on those schedules. That resurrected
  schedules that `end_at` had exhausted.

Schedules with no `max_runs` were not affected. Each guard is now
parenthesized, and the statements touch the target schedule only. Check
`runs_started` and `exhausted_at` on capped schedules in a shard that ran a
trigger-now or a backfill. A schedule may have exhausted early, or resumed
after its `end_at`.
