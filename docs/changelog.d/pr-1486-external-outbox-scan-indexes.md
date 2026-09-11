## Perf — Index both sides of the external signal/cancel/await outbox scans, and pin their plan (issue #1486)

`timeout::enforce_timeouts_once` runs three sibling outbox scanners on every
worker's periodic tick: `enforce_external_signals_outbox`,
`enforce_external_cancels_outbox` and `enforce_external_awaits_outbox`. Each
drains its own outbox of pending cross-workflow `ctx.signal()` /
`ctx.cancel()` / `ctx.await_external()` requests with the same query — claim
one candidate row under `LIMIT 1 ... FOR UPDATE OF e SKIP LOCKED`, act on it,
loop until empty. Neither side of that claim was indexed.

Issue #1486 profiled the outer scan and filed a **negative result**: the
obvious partial index on `event_type` regressed a full drain by 271%, and
raising the statistics target oscillated between plans mid-drain. It
recommended pinning the paired anti-join structurally before re-measuring the
index. This PR does both, and the measurements below confirm the issue's
central claim — each half alone is worse than neither.

**Migration `20260911213344_harvest_external_outbox_scan_indexes`** adds four
partial indexes on `harvest_events`. One,
`idx_harvest_events_external_outbox_pending` on `(event_type, id) WHERE
event_type IN ('ExternalSignalRequested', 'ExternalCancelRequested',
'ExternalAwaitRequested')`, serves all three outer scans: each scanner filters
a single type, which the leading column answers as a prefix. Three more —
`idx_harvest_events_external_{signal,cancel,await}_resolved` — key the
resolution check on `(workflow_exec_id, (event_data->'data'->>'<id>'))`,
partial on that family's two terminal event types. The resolution check was
the larger of the two costs: correlated per execution and unindexed, it
re-read every event of the owning execution once per already-resolved
candidate a drain stepped over, so its cost grew with the square of the
backlog.

**Query rewrite (`timeout.rs`).** The three claim queries are now generated
from one `external_outbox_claim_query!` template, so the shape cannot drift
between siblings. Both joins are pinned to their correlated form by a `LIMIT
1` inside a `LATERAL`, which cannot be pulled up into the outer query, and the
outer scan is pinned by `ORDER BY e.id`, which only the new partial index can
produce. The pins remove the plan's dependence on a row estimate that swings
across the outbox's own draining range. The `NOT EXISTS` becomes `LEFT JOIN
LATERAL ... WHERE res.resolved IS NULL`, which is the same predicate and not a
`NOT IN`: the subquery projects a constant, so the join column is null exactly
when no resolution row matched, and a SQL-NULL correlation id cannot change
the answer. `FOR UPDATE OF e SKIP LOCKED` locks the same single relation as
before, on the non-nullable side of the outer join.

**Measured** on the issue's own fixture (5,000 RUNNING executions x 200
events, plus a 2,000-execution terminal tail, 1.02M rows), full 50-request
drain, `pg_stat_statements` total buffers over 204 statements:

| scenario | buffers | vs baseline |
|:--|--:|--:|
| baseline (no index, legacy query) | 298,967 | — |
| rewrite only, no indexes | 1,604,209 | **+437%** |
| indexes + rewrite | 8,088 | **-97.3%** |

A single cold claim falls from 21,245 buffers (`Seq Scan`, `Rows Removed by
Filter: 1,020,000`) to 9. Under a deliberately stale row estimate — 400x the
truth, the shape an outage backlog leaves behind after `ANALYZE` — the same
drain costs 49,915 buffers before and 10,820 after, while the index-only form
degrades to 37,714 because the planner abandons the partial index for a `Seq
Scan`.

**Behaviour.** No `WorkflowEvent` variant, no column change, no data
migration, no replay impact, and no change to which rows the scanners claim —
`external_outbox_scan_tests::outbox_claim_queries_match_the_legacy_anti_join`
asserts the rewritten SQL selects exactly the legacy row set across every
predicate the rewrite touches, for all three families. The one behaviour
change is drain order: `ORDER BY e.id` is append order, so the oldest pending
request is claimed first instead of an arbitrary one, and a backlog cannot be
starved by newer arrivals.

**Tests.** `autumn-harvest/tests/integration/external_outbox_scan_tests.rs` —
two plan gates (index-only plans, and the same plans under a stale row
estimate), the legacy-equivalence gate, a drain-order gate, and an
`#[ignore]`d evidence capture behind
`autumn-harvest/scripts/external_outbox_scan_perf_repro.sh`. Plans and
`pg_stat_statements` snapshots are committed under
`docs/perf-artifacts/external-outbox-scan/`; the writeup is
`docs/performance-external-outbox-scan.md`.
