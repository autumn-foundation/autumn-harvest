## Tooling — Ledger census batch 2: the per-tick profiles (issue #2396)

The Ledger census now plans the `per_tick` Postgres profiles, the background
loops, scanners and sweepers. 172 of them link a plan page under
`docs/ledger/plans/`. The suites did not reach 23.

`docs/audits/ledger-census.py` now guesses the type of each parameter from
its recorded bind value. When a generic plan fails on `($3)[2]` or
`unnest($1)`, it plans again with casts. It keeps the guessed types once per
statement, and it drops them for a bulk write with more than 64 parameters.

The pass found one real defect. The retention candidate scan lists six
terminal states, but `idx_harvest_we_retention_scan` covers only five. So
the scan cannot use its index and reads all of `harvest_workflow_executions`
on each tick (issue #2395). The same PR fixes it. See
`issue-2395-retention-candidates-index.md`.

One driver is corrected. `audit_export::pending_and_lag` backs an admin
read, so its driver is `per_request`.

The census itself changes no engine code: no new `WorkflowEvent` variant, no
migration and no schema change.
