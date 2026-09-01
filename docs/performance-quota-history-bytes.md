# Quota `history_bytes` admission check: measured, no query-shape fix identified

`quota::load_quota_usage`'s SQL doc comment (issue #946 AC7) states the query
is "cheap by construction... never a full-table scan per admission." This
page measures that claim and finds it **partially inaccurate but not a
performance defect**: below roughly 300k total `harvest_events` rows, the
query does do a full `Seq Scan` of the entire table — and a rewrite that
structurally forces the intended index-bounded plan measures **worse**, not
better, at that same size. This is a **negative finding, not an
optimization**: no code change to `quota.rs` beyond a read-only accessor
(`quota_usage_query()`, mirroring `queue::claim_task_query()`) added so this
evidence capture runs the exact production statement rather than a
hand-copied approximation of it. The doc comment on `QUOTA_USAGE_SQL` itself
has been corrected in the same commit that adds this page.

The real, unavoidable cost this query pays regardless of which plan runs:
computing an exact `SUM(pg_column_size(...))` requires reading every
contributing event row, so cost is proportional to the target tenant's own
accumulated active-execution history — for a tenant near its configured
`max_history_bytes` cap, tens of thousands of buffer touches on *every*
admission, recomputed from scratch each time. See [The cost that
remains](#the-cost-that-remains-and-why-this-is-not-a-ledger-fix) below for
why this is a human decision, not a query rewrite.

## Workload

[`crate::quota::load_quota_usage`] runs once per admission attempt for every
fresh start *and* every spawned child of a workflow type with a declared
`QuotaPolicy` — `execution::enforce_quota_admission` calls it inside the same
transaction as the per-key advisory lock, before the `WorkflowStarted` event
is appended. It is opt-in (AC9: a workflow type with no `QuotaPolicy` pays
nothing), so this page's findings apply only to deployments that declare
`max_history_bytes`.

`tests/integration/quota_history_bytes_perf_tests.rs::zz_capture_quota_history_bytes_evidence`
seeds a production-shaped fixture: one target tenant
(`workflow_name = 'order_saga'`, `quota_key = 'acme'`) with 1,000 active
(`RUNNING`/`PAUSED`) executions whose event-history length is **deterministically
skewed** by execution index rather than uniform — no `random()`, so the
fixture and every downstream number reproduce byte-for-byte on every run:

| share of 1,000 executions | events per execution | role |
|---:|---:|---|
| 5% (`i % 20 == 0`) | 2,000–2,499 | long-running saga tail |
| 20% (`i % 20` in 1..=4) | 200–299 | medium workflow |
| 75% | 10–29 | typical short workflow |

This totals **178,000 events / 80,192,528 bytes (~80.2 MB)** of history for
the target tenant, plus 50 dead-letter rows. A sweep (`NOISE_SWEEP = [3, 15,
100]`) independently scales *background* tenants sharing the same tables —
other `quota_key`s with a light, uniform history — so total `harvest_events`
table size lands at roughly 205k / 313k / 1.08M rows while the target
tenant's own footprint stays fixed. This mirrors how the table actually grows
in production: more tenants accumulate over time; one tenant's own active
footprint is bounded by the very cap this feature enforces.

## Profile

This is a single query (`load_quota_usage`), not a multi-statement path, so
there is no `calls`-ranking story the way `docs/performance.md`'s claim-path
passes have — the interesting variable is *plan shape as the surrounding
table grows*, not attribution across statements. Every number below is
either a direct `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` of
`quota::quota_usage_query()` — the exact string `load_quota_usage` executes —
or a `pg_stat_statements` snapshot after driving the real, compiled
`load_quota_usage()` function.

## Plan

The `history_bytes` `InitPlan` is the one that matters; `active_executions`
and `dead_letters` are cheap partial-index lookups at every size tested
(under 30 buffers each) and are not discussed further.

| total `harvest_events` rows | plan for `history_bytes` | buffers (hit+read) |
|---:|---|---:|
| 205,000 | **`Seq Scan` of the entire table**, then `Hash Join` against the 1,000-row active set | 12,745 |
| 313,000 | `Nested Loop` over `active`, `Index Scan using idx_harvest_events_exec_last`, 1,000 loops | 16,766 (5,374 hit + 11,392 read) |
| 1,078,000 | `Nested Loop` over `active`, `Index Scan using idx_harvest_events_exec_last`, 1,000 loops | 16,768 (10,243 hit + 6,525 read) |

Full captured plans: `docs/perf-artifacts/quota-history-bytes-admission/noise_mult-{3,15,100}.explain.txt`.

Two things this table shows:

1. **The planner already switches plans on its own** as the table grows past
   roughly 300k rows — from a full `Seq Scan` to the index-bounded `Nested
   Loop` the doc comment describes. This happens with **zero code change**;
   `quota_usage_query()`'s `WHERE e.workflow_exec_id IN (SELECT id FROM
   active)` is left exactly as it ships.
2. **Once on the `Nested Loop` plan, buffer cost is flat** across a 3.4x
   growth in total table size (313k → 1,078,000 rows: 16,766 → 16,768
   buffers, effectively unchanged). This confirms the query, on its intended
   plan, costs what the target tenant's own active footprint costs — not
   what the whole table costs.

The row-estimate/actual mismatch also grows as background noise grows —
worth citing as a secondary, uninvestigated finding, not this page's main
subject:

| total rows | estimated rows for `history_bytes`'s join | actual rows | ratio |
|---:|---:|---:|---:|
| 205,000 | 55,207 | 178,000 | 3.2x under |
| 313,000 | 22,685 | 178,000 | 7.8x under |
| 1,078,000 | 14,586 | 178,000 | 12.2x under |

The underestimate does not change which plan is chosen at the sizes
measured, but it is the kind of drift that could tip a borderline choice the
wrong way at a size not tested here; extended statistics on
`harvest_events.workflow_exec_id` were not investigated.

## Hypothesis

Below the crossover, the query does a full-table `Seq Scan` because the
planner correctly estimates that scanning the whole (small) table
sequentially is cheaper than 1,000 separate index probes. **Is that scan
actually the more expensive choice once the table is realistically large in
production, or does the planner keep making the right call?** Measured: the
planner keeps making the right call — the crossover already happens well
below typical production table sizes (300k rows is a modest, early-life
`harvest_events` table), and the query's cost is flat past that point
regardless of how much bigger the table gets.

## Negative result: forcing the plan shape structurally

**Hypothesis tested:** replace `WHERE e.workflow_exec_id IN (SELECT id FROM
active)` with a `CROSS JOIN LATERAL` per-active-row correlated aggregate, so
the intended index-bounded access is the *only* plan Postgres can produce —
independent of the planner's cost-based crossover, structurally guaranteeing
AC7's "never a full-table scan" regardless of table size:

```sql
SELECT
    (SELECT COUNT(*) FROM active)::BIGINT AS active_executions,
    COALESCE(
        (SELECT SUM(ev.bytes)
         FROM active a
         CROSS JOIN LATERAL (
             SELECT COALESCE(SUM(pg_column_size(e.event_data)), 0) AS bytes
             FROM harvest_events e
             WHERE e.workflow_exec_id = a.id
         ) ev),
        0
    )::BIGINT AS history_bytes,
    ...
```

**Measured** at the smallest fixture (205,000 total rows — the one size
where the unmodified query currently picks the `Seq Scan`, and therefore the
one size where a forced rewrite would matter if it helped):

| variant | plan | buffers (hit+read) |
|---|---|---:|
| unmodified (`IN` subquery) | `Seq Scan` of the whole table | 12,745 |
| `LATERAL` rewrite | `Nested Loop` → `Bitmap Heap Scan` via `idx_harvest_events_exec`, 1,000 loops | 15,765 (15,731 hit + 34 read) |

The rewrite **costs 24% more**, not less. Postgres's per-row plan for the
correlated form is a `Bitmap Heap Scan` (build a bitmap, sort, then fetch) —
more expensive per probe at this row count (~178 rows/execution) than either
the unmodified query's own sequential scan at this size, or the plain
`Index Scan` the *unmodified* query reaches on its own once the table is
larger (see the Plan table above: 16,766–16,768 buffers on a table 1.5x–5.3x
bigger). Forcing the plan shape traded a cost that already goes away on its
own past 300k rows for a permanent tax below that point.

**Verdict: reverted.** `quota_usage_query()` ships unmodified. Full captured
plan: `docs/perf-artifacts/quota-history-bytes-admission/lateral-variant-negative-result.explain.txt`.
Recording this here so nobody retries the same `LATERAL` rewrite.

## Change

**None**, beyond the read-only `quota::quota_usage_query()` accessor (so this
evidence capture, like `queue::claim_task_query()`'s precedent, runs the
literal production SQL string) and a correction to `QUOTA_USAGE_SQL`'s doc
comment reflecting the findings on this page. `load_quota_usage`'s query text
and `enforce_quota_admission`'s call pattern are unchanged.

## Measurement

| method | value |
|---|---:|
| `EXPLAIN` buffers @ 205,000 total rows (Seq Scan) | 12,745 |
| `EXPLAIN` buffers @ 313,000 total rows (Nested Loop) | 16,766 |
| `EXPLAIN` buffers @ 1,078,000 total rows (Nested Loop) | 16,768 |
| `pg_stat_statements`, 20 real `load_quota_usage()` calls @ 1,078,000 rows | 362,223 total buffers (18,111/call), mean 57.8ms |
| `LATERAL` rewrite @ 205,000 total rows | 15,765 (vs. 12,745 unmodified — **worse**) |

Tool used for every row: `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` and
`pg_stat_statements`, both against a fresh, fully-migrated database
(`autumn_harvest::full_migrations_sql()`), not a hand-built schema subset.
Full artifacts: `docs/perf-artifacts/quota-history-bytes-admission/`.

## Equivalence

No behavior changed — this page ships no query rewrite. The evidence-capture
test itself asserts `load_quota_usage`'s returned `(active_executions,
history_bytes, dead_letters)` equals the fixture's known-exact values
(`1000`, `80192528`, `50`) at all three sweep points, as a correctness sanity
check that reseeding between sizes never drifts the target tenant's own
footprint.

## Write cost

None: no index added, no schema change.

## The cost that remains, and why this is not a Ledger fix

Even on its best (and, as shown above, self-selected) plan, this query costs
~16,700–18,100 buffers and ~50–100ms **per admission**, and that cost is
proportional to the target tenant's own accumulated active-execution
history — recomputed from scratch, synchronously, inside the admission
transaction, on *every single* fresh start and spawned child for that
tenant. A tenant sitting anywhere near its configured `max_history_bytes` cap
— exactly the tenant this feature exists to protect against — pays this on
every admission attempt, forever, and the bill grows monotonically with that
tenant's own footprint. There is currently no upper bound on this
per-admission cost.

A structural fix exists in principle: an incrementally-maintained running
byte counter per `(workflow_name, quota_key)`, updated on event append and
decremented on execution completion/collection, would flatten this to O(1)
per admission. That is a **denormalized rollup column**, which this
project's own performance-review guidance requires "both a measured read win
and a stated invariant for how it stays correct" for — and the invariant
here would need to hold across concurrent appenders, `PAUSED`/`RESUMED`
transitions, retention collection (issue #958 / PR #1264's partitioned event
storage — confirmed by that PR's own "reads do not partition-prune" caveat
to neither fix nor worsen this query, since it filters on `workflow_exec_id`
rather than the partition's `cohort` column), and `quota_key` changes on
`continue_as_new`
(`continue_as_new_cross_type_re_resolves_quota_key`/`_clears_quota_key` in
`quota_enforcement_tests.rs`). That is exactly the class of change flagged
for a human decision, not an automated pass — recording it here as a
follow-up candidate rather than attempting it.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/quota_history_bytes_perf_repro.sh
```

Or directly:

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored zz_capture_quota_history_bytes_evidence --nocapture
```

## See also

- `docs/performance.md` — the claim-path passes this page's methodology
  follows.
- `docs/performance-capability-labels.md` — the precedent for a "measured,
  no fix" negative-result page, including the three-independent-methods
  measurement pattern this page reuses.
- `docs/performance-history-bloat-filter.md` — the closest prior art for a
  threshold-style aggregate over `harvest_events`; that page's `bounded
  EXISTS` fix does not transfer here because `max_history_bytes` needs an
  *exact* sum, not a boolean threshold, and the common case (a tenant under
  its cap) cannot early-exit regardless.
