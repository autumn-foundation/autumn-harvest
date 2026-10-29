## Tooling — Ledger census: the access path of every per-task query (issue #2366)

A new census plans each SQL statement that the DB suites send, and maps it to
its Ledger profile. A Diesel instrumentation hook in the integration test
binary records each statement and its call stack when `HARVEST_LEDGER_CENSUS`
names a file. `docs/audits/ledger-census.py` maps the stack to a profile and
runs `EXPLAIN (GENERIC_PLAN)` with `enable_seqscan = off` on a migrated
database. Bool and string bind values replace their parameters, as in a custom
plan, so a partial index such as `WHERE NOT consumed` applies.

The first pass covers the `per_task` Postgres profiles. The census reached 144
of the 175, and each one now has a `plan` field that links its page under
`docs/ledger/plans/`. One statement has no usable index:
`signal::forward_signals_to_retry_attempt` scans all of `harvest_signals` on
each workflow retry (issue #2367).

`ledger-profiles.py` accepts the new optional `plan` field and checks that its
page names the function. CI runs the census self-test in the `lint` job.

No engine change: no new `WorkflowEvent` variant, no migration, no schema
change and no public API change.
