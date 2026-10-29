## Tooling — Ledger census batch 3: the per-request profiles (issue #2402)

The Ledger census now plans the `per_request` Postgres profiles, the queries
behind API calls and operator reads. 153 of them link a plan page under
`docs/ledger/plans/`. The suites did not reach 173. Most of those are in
`autumn-harvest-plugin`, whose tests run in separate binaries that the census
hook does not cover yet.

The pass found one new defect. Dead-letter list pages and callback redrives
have no usable index on `harvest_dead_letters`, so each one reads the whole
DLQ (issue #2401). It also found a second caller for issue #2367: PII erasure
updates `harvest_signals` by execution, with no usable index.

The census itself changes no engine code: no new `WorkflowEvent` variant, no
migration and no schema change.
