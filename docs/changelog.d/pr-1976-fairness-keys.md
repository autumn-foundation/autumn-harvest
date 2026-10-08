## Phase 8.x — Weighted fairness keys within a queue (issue #1976)

A task can carry a fairness key. A worker with
`WorkerConfig::with_fairness_keys(true)` serves the keys of each queue in
weighted round robin. One tenant's flood can no longer hold another tenant's
tasks in a shared queue. Off by default; off, the claim statement is
byte-identical.

- **Keys.** `StartWorkflowParams::fairness_key`,
  `TypedStartOptions::fairness_key`, and a `fairness_key` field on the HTTP
  start body and on batch-start items. With no key, the run's quota key is
  the key. Activities, children, continue-as-new runs and workflow retries
  inherit it. Reset forks and DLQ redrives take the quota key.
- **Claim.** Start-time fair queuing. `queue::splice_fairness` adds one
  `MATERIALIZED` CTE (a `jsonb` map of key lags), one sort term (the key's
  lag) after the effective priority, and one state upsert. No new bind. New entry points:
  `claim_task_with_fairness`, `claim_task_by_id_with_fairness`.
- **Runtime weights.** `fairness_keys::{set,clear,list}_fairness_weight(s)`,
  `GET/POST/DELETE /admin/queues/{queue}/fairness[/{key}]` and
  `harvest queue fairness {show,set,clear}`. Weight 0.001 to 1000; at most
  1,000 overrides per queue. A change applies at the next claim. Audited.
- **Upkeep.** The retention janitor prunes idle state rows that the claim
  cannot tell from no row.
- **Invariants.** No new `WorkflowEvent` variant. Migration
  `20261008040947_harvest_fairness_keys` adds a nullable column and two
  tables. ADR 0006 records the design.
- **Evidence.** Red test
  `tenant_flood_holds_tenant_b_within_the_bound_with_fairness_keys` (B waited
  199 claims before; at most 1 after). Property proofs in
  `fairness_key_props`. Worker end-to-end: B completes after 1 of 20 flood
  runs with fairness on, after 20 of 20 with it off. The claim benchmark
  `fairness_keys` section measures the cost (see `docs/performance.md`).
