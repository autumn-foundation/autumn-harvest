# External signal/cancel/await outbox scans indexed and pinned (issue #1486)

`timeout::enforce_timeouts_once` runs three sibling outbox scanners on every
worker's periodic tick. Neither side of their claim query was indexed, so each
claim read the whole event table. Issue #1486 profiled that gap, measured the
obvious index fix as a **271% regression**, and filed a findings issue rather
than a PR. This page measures the fix that works: the index and a query
rewrite, together, because each alone is worse than neither.

## 🎯 Workload

`enforce_external_signals_outbox` (`timeout.rs`), and its `..._cancels_...` and
`..._awaits_...` siblings. Each drains its own outbox of pending cross-workflow
`ctx.signal()` / `ctx.cancel()` / `ctx.await_external()` requests: claim one
candidate row under `LIMIT 1 ... FOR UPDATE OF e SKIP LOCKED`, deliver it,
append a terminal event, loop until the outbox is empty.

The three queries are byte-for-byte the same shape. They differ in the request
type they claim, the two events that resolve it, and the payload key that
correlates them. The workload profiled here is one full drain of one outbox --
51 claims and 50 delivery markers -- which is what a worker does after any
period where requests accumulated.

The fixture is the one issue #1486 specified: 5,000 RUNNING executions with 200
events each, a 2,000-execution terminal tail with 10 events each, and 50
unresolved `ExternalSignalRequested` rows spread across the RUNNING population.
1,020,050 event rows in total -- a long-running-workflow population, which is
what this scanner searches.

## 📈 Profile

`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` of one cold claim, before:

```
->  Seq Scan on public.harvest_events e  (actual rows=1 loops=1)
      Filter: ((e.id <> ALL ('{}'::bigint[])) AND (e.event_type = 'ExternalSignalRequested'::text) AND ...)
      Rows Removed by Filter: 1020000
      Buffers: shared hit=7307 read=13730
```

21,037 buffers to find the one row `LIMIT 1` needed, out of the claim's 21,246
total. The cost scales with the size of the event table, not with the backlog
being searched.

`harvest_events` already carries partial indexes for exactly this problem on
other rare event types -- `idx_harvest_events_activity_type_ts` and
`idx_harvest_events_reset_terminated`, both from
`20260702000000_harvest_usage_report_indexes`. Nothing analogous existed for
these three.

**The outer scan is not the larger cost.** Across a full drain the claim
statement spent 298,123 buffers over 51 calls, an average of 5,845 -- well
above the 21,246 a single cold claim costs, because the resolution check is
paid again on every already-resolved candidate a later claim steps over. That
check is correlated per execution and was unindexed, so each probe re-read
every event of the owning execution. Its cost grows with the square of the
backlog.

## 🧭 Plan: why the obvious fix regressed

Issue #1486 measured the partial index on the outer predicate by itself and
reported a full-drain regression of 271%, plus an unreliable oscillation when
the statistics target was raised. That result is real, and it has a mechanism:
the outer index changes the row estimate the planner uses to cost the paired
`NOT EXISTS`, and that estimate sits at this query's own
correlated-versus-materialised crossover. Past the crossover the planner stops
correlating the anti-join and reads the whole table once instead, which a drain
loop then pays on every iteration.

The issue's recommendation was to pin the anti-join structurally first, then
re-measure the index. That is what this change does.

## 🔧 Change

**Migration `20260911213344_harvest_external_outbox_scan_indexes`** -- four
partial indexes on `harvest_events`:

```sql
CREATE INDEX idx_harvest_events_external_outbox_pending
    ON harvest_events (event_type, timestamp, id)
    WHERE event_type IN ('ExternalSignalRequested',
                         'ExternalCancelRequested',
                         'ExternalAwaitRequested');

CREATE INDEX idx_harvest_events_external_signal_resolved
    ON harvest_events (workflow_exec_id, (event_data->'data'->>'signal_id'))
    WHERE event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed');
-- ... and the cancel and await equivalents, keyed on `cancel_id` / `await_id`.
```

One index serves all three outer scans: each scanner filters a single
`event_type`, which the leading column answers as a prefix. Three more key the
resolution check exactly as that check is written.

**Query rewrite (`timeout.rs`)** -- the three claim queries are now generated
from one `external_outbox_claim_query!` template, so the shape cannot drift
between siblings. Both joins are pinned to their correlated form:

```sql
SELECT e.* FROM harvest_events e
JOIN LATERAL (
    SELECT 1 AS running_exec FROM harvest_workflow_executions x
    WHERE x.id = e.workflow_exec_id AND x.state = 'RUNNING'
      AND x.shard_id = ANY($1)
    LIMIT 1
) running ON TRUE
LEFT JOIN LATERAL (
    SELECT 1 AS resolved FROM harvest_events res
    WHERE res.workflow_exec_id = e.workflow_exec_id
      AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed')
      AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id'
    LIMIT 1
) res ON TRUE
WHERE e.event_type = 'ExternalSignalRequested'
  AND (e.event_data->'data'->>'signal_id') IS NOT NULL
  AND NOT (e.id = ANY($2))
  AND res.resolved IS NULL
ORDER BY e.timestamp, e.id
LIMIT 1
FOR UPDATE OF e SKIP LOCKED
```

A `LIMIT 1` inside a `LATERAL` cannot be pulled up into the outer query, so the
correlated shape is structural rather than a plan the planner happens to prefer
today. The `INNER JOIN` on `harvest_workflow_executions` became a `LATERAL`
too, for the same reason and for a measured one: once the anti-join moves, the
planner re-shapes that join into a hash join and seq-scans the executions
table.

`ORDER BY e.timestamp, e.id` pins the outer scan. Only the new partial index
produces that order, so every competing plan needs an explicit sort, and a sort
under `LIMIT 1` must read every candidate before it can return one. The ordered
index scan returns after one row instead.

**`ORDER BY e.id` alone does not pin it**, and this was measured rather than
assumed. `harvest_events_pkey` supplies `id` order too, so under an inflated
estimate the planner walked the primary key, filtering every unrelated event
out of a full ascending scan -- not a `Seq Scan`, and just as expensive.
Prefixing the order with `event_type` does not help either: the planner drops a
column that the `WHERE` clause pins to a constant from the ordering it has to
satisfy, which makes the primary key eligible again. `timestamp` is neither
constant nor served by any other index on this table.

## 📊 Measurement

Full 50-request drain, `pg_stat_statements` total buffers over 204 statements,
Postgres 16.13. Two independent harnesses agree: the committed artifacts come
from the Rust evidence-capture test, and the four-way matrix below from a plain
`psql` harness over the same fixture.

| scenario | buffers | vs baseline |
|:--|--:|--:|
| baseline -- no index, legacy query | 299,018 | -- |
| **rewrite only**, no indexes | 1,604,212 | **+436%** |
| indexes only, legacy query | 8,088 | -97.3% |
| **indexes + rewrite** | 8,088 | **-97.3%** |

The rewrite-only row is the reason these ship together. Three correlated
`LATERAL` probes with nothing to serve them is 5.4x worse than doing nothing.

The committed evidence capture, through the real query text and the real drain
loop, measured 299,263 buffers before and 7,857 after -- **-97.4%** -- with the
same 50 requests resolved by both forms.

Single cold claim:

| scenario | buffers | dominant node |
|:--|--:|:--|
| before | 21,246 | `Seq Scan on harvest_events` (21,037 buf, `Rows Removed by Filter: 1,020,000`) |
| after | 8 | none -- every node is a keyed index scan |

### Under a stale row estimate

The fresh-statistics rows above show the indexes carrying the win and the
rewrite adding nothing. That is the easy case, and measuring only it would have
published plan-dependent luck. The hard case is the one issue #1486 ran into,
and it has a cause a deployment meets in practice: an outage fills the outbox,
autovacuum's `ANALYZE` records the backlog, the backlog drains, and the stored
estimate stays orders of magnitude above the truth.

Same fixture, plus 200,000 historical resolution events, with the row estimate
left 400x above the real pending count:

| scenario | buffers | vs baseline |
|:--|--:|--:|
| baseline | 49,915 | -- |
| indexes only, legacy query | 37,714 | -24.4% |
| **indexes + rewrite** | 10,820 | **-78.3%** |

The index-only form loses most of its win here because the planner abandons the
partial index. The pinned form does not depend on the estimate at all.

## ✅ Equivalence

`external_outbox_scan_tests::outbox_claim_queries_match_the_legacy_anti_join`
runs the pre-#1486 SQL and the rewritten SQL against the same fixture, for all
three families, and asserts they select the same rows. The fixture covers every
predicate the rewrite touches, including the two the issue named as needing
their own correctness review:

* **`NOT EXISTS` NULL semantics.** `LEFT JOIN LATERAL ... WHERE res.resolved IS
  NULL` is the same predicate, not a `NOT IN`: the subquery projects a
  constant, so the join column is null exactly when no resolution row matched.
  The fixture seeds a request whose correlation key is absent and one whose key
  is JSON null; both stay excluded, as they were before.
* **The `FOR UPDATE OF e SKIP LOCKED` contract.** The rewrite locks the same
  single relation, and `e` remains on the non-nullable side of the outer join,
  which is the case Postgres permits. Nothing else in the query is locked.

The evidence capture asserts equivalence a second way, over a full drain: both
forms resolve the same 50 requests.

One behaviour does change. `ORDER BY e.timestamp, e.id` makes the drain order
oldest-first instead of arbitrary, so a backlog cannot be starved by newer
arrivals. `outbox_claim_returns_the_oldest_pending_request_first` asserts it.

## 💸 Write cost

All four indexes are partial on event types that are rare in any history, so
the cost falls on those rows alone.

* Index size on the 1.02M-row fixture: 16 kB for the pending index over its 50
  rows, 8 kB for each empty resolution index.
* WAL for 10,000 `ExternalSignalRequested` inserts: 37,232,952 bytes without
  the indexes, 38,582,616 with -- **+3.6%**, on the request path only.
* WAL for 10,000 `ActivityStarted` inserts, an event type none of these indexes
  covers: 19,271,112 without and 19,133,120 with. No systematic cost, which is
  what partial scope is supposed to buy.

`CREATE INDEX` (not `CONCURRENTLY`) takes `SHARE` on `harvest_events` for the
build. On a live, already-large deployment, build the four out of band first
and this migration's guard accepts them; the recipe, including the partitioned
layout's per-leaf variant, is written out in full in
`20260905181020_harvest_usage_activity_lookback_index/up.sql`.

## 🔬 Reproduce

```bash
# Fast, always-run gates (plans, legacy equivalence, drain order):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --features db --test integration external_outbox_scan

# Full evidence capture (seeds 1.02M event rows; about 40 seconds):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/external_outbox_scan_perf_repro.sh
```

Artifacts land in `docs/perf-artifacts/external-outbox-scan/`.

## 🚧 Known limitations

* **The drain still re-probes resolved candidates.** The claim statement
  averages 132 buffers per call across the drain, against 8 for a cold claim,
  because each claim steps over every candidate a previous claim already
  resolved. The probe is now an indexed lookup, so the constant is small, but
  the shape is still quadratic in the backlog. Closing it means adding
  successfully-resolved ids to the sweep's `excluded_event_ids` list, which is
  a change to the scanner loop rather than to its query, and is not measured
  here.
* **Measured in isolation.** This is the scanner's own cost, not its share of a
  mixed end-to-end workload. The same limitation issue #1486 stated stands.
* **The cancel and await siblings are covered by construction, not by their own
  drain measurement.** All three queries come from one template and are gated
  by the same three tests, so a plan or result-set difference between them
  fails CI. Only the signal family's drain was profiled.

## See also

* [`docs/performance-usage-report-activity-lookback.md`](performance-usage-report-activity-lookback.md)
  -- the other `harvest_events` expression index, and the out-of-band build
  recipe this migration points at.
* [`docs/performance.md`](performance.md) -- the index of profiling passes.
