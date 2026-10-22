## Phase — SLA-breach scan uses its partial index (Ledger)

**What changed.** `timeout::enforce_workflow_sla_breaches` repeats the tail of
the predicate of `idx_harvest_executions_sla_deadline`, `(completed_at IS NULL
OR completed_at > sla_deadline_at)`, in its `WHERE`. The new clause adds no
condition. The first clause already implies it.

**Why.** Postgres cannot prove that `sla_deadline_at < COALESCE(completed_at,
NOW())` implies the index predicate, so the partial index was never a
candidate. Every timeout tick read the whole executions table to flag zero
rows: 189, 1,887 and 19,887 buffers at 10,000, 100,000 and 1,000,000
executions. It now reads 1 buffer at every size.

**Evidence.** `docs/performance-sla-breach-scan.md` and
`docs/perf-artifacts/sla-breach-scan/`. The flagged rows are identical before
and after across 14 boundary rows at three table sizes. The existing
`sla_breach_tests` pass unchanged.

No `WorkflowEvent` variant, no migration, no schema change, no replay impact.
