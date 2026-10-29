## Tooling — Ledger census batch 4: the setup profiles (issue #2414)

The Ledger census now plans the `setup` Postgres profiles, the queries that
run at startup, in migrations and in partition layout changes. 63 of them link
a plan page under `docs/ledger/plans/`. The suites did not reach 62.

No setup statement is a defect. The ones with no usable index read the system
catalog, rewrite all of `harvest_events` during a layout change on purpose, or
sync the small completion-trigger registry at startup.

The census itself changes no engine code: no new `WorkflowEvent` variant, no
migration and no schema change.
