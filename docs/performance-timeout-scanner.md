# The timeout scanner's own queries: measured, no fix identified

`docs/performance.md`'s "Known limitations" section flags this directly:
"The scheduler tick and the timeout scanner are not benchmarked here. They
are separate hot paths and separate work." This page is that measurement for
`timeout::enforce_timeouts_once` — the tick that runs continuously, on every
shard, whether or not there is anything to do.

The result is a **negative finding, not an optimization**: every scanner
query `enforce_timeouts_once` issues is already served by an existing
partial index or an already-cheap plan at realistic scale. No query-shape
fix, index, or `MATERIALIZED` rewrite is proposed here, and none is needed
by the evidence below. Per the acceptable-outcomes rule ("optimization PR,
findings issue, negative result — all are successful runs; do not open a PR
to demonstrate activity"), this ships as a measured, reproducible finding: a
harness, committed baseline artifacts, and this writeup, with no code change
to `timeout.rs`.

## Hypothesis

`queue::claim_task_query()`'s queue-pause anti-join was, before its fix (see
`docs/performance.md#the-queue-pause-anti-join-fix`), a correlated
`NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp WHERE qp.queue_name =
harvest_task_queue.queue_name)` — re-evaluated once per candidate row the
outer scan visited, costing `loops=10000` buffer hits at the published
headline backlog.

`timeout::schedule_to_start_timeout_query()` (issue #378/#619/#807) contains
**the same two anti-joins in the same textual shape**, unfixed:

```sql
SELECT t.* FROM harvest_task_queue t
WHERE t.state = 'PENDING'
  AND t.schedule_to_start IS NOT NULL
  AND t.scheduled_at + t.schedule_to_start < NOW()
  AND NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp
      WHERE qp.queue_name = t.queue_name)
  AND NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap
      WHERE ap.activity_name = t.activity_name
        AND t.task_type = 'activity')
  AND NOT (...)
```

This scanner runs on every timeout tick — every 50ms–5s (configurable),
continuously, on every shard, whether or not anything is actually
timed-out — which is a different exposure than the claim query's per-poll
cost. The hypothesis: if Postgres re-evaluates either anti-join per
candidate row here too, the same `loops=N` cost the claim query had would
recur, unfixed, on a hot recurring background path.

## Measurement

**It does not reproduce.** `harvest_queue_pauses` and `harvest_activity_pauses`
are tiny (row count = number of *currently paused* queues/activity types —
almost always in the single digits, PK-indexed on the exact column the
anti-join probes). At that cardinality, Postgres's planner resolves both
anti-joins via a one-time `Materialize` node — the whole pause table read
once and cached, then probed from memory per candidate row with **zero**
additional buffer reads per probe — rather than a per-row indexed lookup.
This is a *different*, already-cheap plan shape from the claim query's
pre-fix `Nested Loop Anti Join` with `Index Scan ... (loops=10000)`, and it
holds at every backlog depth tested (see the committed EXPLAIN output).

The query's own base-table scan is the other place a correlated cost could
hide, and it does not: `harvest_task_queue` in a busy, not-yet-retention-swept
production deployment is dominated by terminal (`COMPLETED`/`FAILED`) rows
from long-lived workflows' completed activities — `PENDING`/`RUNNING` rows
are a small, worker-concurrency-bounded fraction of the table. Every scanner
query's leading predicate (`state = 'PENDING'` or `state = 'RUNNING'`) is
served by an existing partial index (`idx_harvest_tq_poll`,
`idx_harvest_tq_running`), so Postgres reads only the live population, not
the terminal bulk, regardless of how large the table's history grows.

Reproduced against a fixture with realistic cardinality skew — 300,000
terminal `harvest_task_queue` rows, 2,000 `RUNNING`, 5,000 `PENDING` (this
repo's own published claim-bench headline backlog depth), one paused queue,
one paused activity, and 100,000 `RUNNING` `harvest_workflow_executions`
rows with a sparse `deadline_at`/`chain_deadline_at`/`sla_deadline_at`
population:

| Query | Plan shape | Buffers, execution (root node) | Buffers, planning |
|:--|:--|--:|--:|
| `heartbeat_timeout_query()` | `Index Scan` on `idx_harvest_tq_running` (Filter for `heartbeat_timeout`) | 48 | 251 hit + 8 read |
| `start_to_close_timeout_query()` | `Index Scan` on `idx_harvest_tq_running` (Filter for `start_to_close`) | 48 | 8 |
| `schedule_to_start_timeout_query()` | `Bitmap Index Scan` on `idx_harvest_tq_poll` → `Bitmap Heap Scan` (Filter) → two `Materialize`-backed anti-joins + one `SubPlan` (frozen-row carve-out, `loops=25`) | 211 | 220 |
| `schedule_to_close_timeout_query()` | `Index Scan` on `harvest_task_queue_schedule_to_close_idx` → `Nested Loop Anti Join` against `harvest_workflow_executions_pkey` (`loops=25`) | 126 | 72 |
| `workflow_execution_timeout_query()` | `BitmapOr` of `idx_harvest_executions_deadline` + `idx_harvest_executions_chain_deadline` → `Bitmap Heap Scan` | 316 | 173 |

(Planning buffers are catalog-lookup cost — normal, one-time-per-plan-cache-miss overhead, not part of the mechanism under test; they are reported for completeness since `SETTINGS` is part of the committed `EXPLAIN` invocation.)

Every number here is buffers, not wall-clock — `EXPLAIN (ANALYZE, BUFFERS,
VERBOSE, SETTINGS, TIMING OFF)` output for all five queries, driven verbatim
from `autumn_harvest::timeout`'s own `pub const fn` query builders (so this
can never drift out of sync with the compiled query text), plus a
`pg_stat_statements` snapshot after one real `enforce_timeouts_once()` call
against the same fixture, are committed under
[`docs/perf-artifacts/timeout-scanner-queries/`](perf-artifacts/timeout-scanner-queries/).

**Why an earlier, unrealistic probe looked alarming.** A first pass seeded
`harvest_task_queue` at 100% `RUNNING` (no terminal bulk at all) to isolate
the heartbeat/start-to-close predicates, and Postgres chose a **Parallel Seq
Scan of the whole table** for both — because the `state = 'RUNNING'` partial
index selects effectively 100% of an all-`RUNNING` table, so the planner
correctly prefers reading the heap directly over an index that buys no
selectivity. That result does not describe production: a real deployment's
`RUNNING` population is bounded by worker concurrency, not by table size, so
it is always a small fraction of a table that also holds completed history.
Re-run at the realistic 2,000-RUNNING-of-302,000-total ratio above, both
queries use `idx_harvest_tq_running` and cost ~50 buffers each. This is
recorded here as a caution for whoever benchmarks this next: an unrealistic
state distribution can manufacture a regression that a production-shaped
fixture does not show, in either direction.

`schedule_to_close_timeout_query()` also carries a correlated-shaped anti-join
— `NOT EXISTS (SELECT 1 FROM harvest_workflow_executions e WHERE e.id =
t.workflow_exec_id AND e.state = 'PAUSED')`, which the captured plan shows as
a `Nested Loop Anti Join` probing `harvest_workflow_executions_pkey` once per
outer row (`loops=25` in this run). This is structurally the same "probe
per candidate row" shape the claim-query fixes eliminated, and it is
correctly **not** flagged as a problem here: the outer side is already
filtered down to `schedule_to_close_at < NOW()` via a dedicated partial
index before the anti-join runs, so `loops` is bounded by how many tasks
have actually blown their total deadline — inherently small in any healthy
fleet — never by `PENDING`/`RUNNING` backlog depth the way the claim query's
pre-fix anti-join was. Each probe is also a plain primary-key lookup (a few
buffers), not a table scan. The mechanism that made the claim-query
anti-joins expensive was *loop count scaling with backlog depth*, not
"anti-join per row" in general — and that scaling is absent here by
construction.

## Why no fix is proposed

The impact floor requires, among other things, "≥20% reduction in total
buffers for a statement that is ≥5% of workload buffers." Every scanner
query measured here is already in the tens-to-low-thousands of buffers
against a fixture sized to this repo's own published claim-bench headline
depth — nowhere near the 10⁵–10⁹-buffer regime the claim query's pre-fix
concurrency-key and queue-pause anti-joins occupied before their fixes (see
`docs/performance.md`). There is no correlated per-row cost to eliminate,
because Postgres already avoids it structurally (tiny anti-join tables
resolved via `Materialize`, not a `loops=N` probe) and no missing index to
add, because the leading `state` predicate on every scanner query is already
partial-indexed.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/timeout_scanner_perf_repro.sh
```

Needs either `HARVEST_TEST_DATABASE_URL` (an admin connection string) or a
reachable Docker daemon for the harness's own testcontainer fallback — not
both. Writes `docs/perf-artifacts/timeout-scanner-queries/`.

## See also

* `docs/performance.md` — the claim-path measurement this page's hypothesis
  was seeded from, including
  [the queue-pause anti-join fix](performance.md#the-queue-pause-anti-join-fix)
  this page checked for a recurrence of.
* `autumn-harvest/tests/integration/timeout_scanner_perf_repro.rs` — the
  evidence-capture test.
* `autumn-harvest/scripts/timeout_scanner_perf_repro.sh` — regenerates the
  committed evidence from a clean checkout.
* `docs/perf-artifacts/timeout-scanner-queries/` — committed
  `EXPLAIN`/`pg_stat_statements` evidence.
