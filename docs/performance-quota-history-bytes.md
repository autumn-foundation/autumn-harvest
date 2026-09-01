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
is appended, **but only when the policy has a declared cap and the resolved
quota key is present**: `enforce_quota_admission` returns before calling
`load_quota_usage` at all when `quota_policy` is `None`, when
`policy.has_any_cap()` is `false`, or when the key resolver produces no key
for this admission's input (`quota_key` is `None`) — see AC9 and
`resolve_quota_key`'s fail-open contract. Within that scope, **the trigger is
`QuotaPolicy::has_any_cap()` — any declared cap — not specifically
`max_history_bytes`.** `load_quota_usage` computes all three counters
(`active_executions`, `history_bytes`, `dead_letters`) in one round trip by
design (AC7), so a workflow type declaring only `max_active_executions` or
only `max_dead_letters` pays this page's full `history_bytes` cost on every
*key-resolved* admission too, even though that counter never factors into
its own admit/reject decision. This page's findings apply to every admission
whose workflow type declares any `QuotaPolicy` cap **and** whose input
resolves to a quota key — not only ones using `max_history_bytes`, and not
to admissions that fail open on an unresolvable key.

`tests/integration/quota_history_bytes_perf_tests.rs::zz_capture_quota_history_bytes_evidence`
seeds a production-shaped fixture: one target tenant
(`workflow_name = 'order_saga'`, `quota_key = 'acme'`) with 1,000 active
(`RUNNING`/`PAUSED`) executions whose event-history length is **deterministically
skewed** by execution index rather than uniform — no `random()`, so the
seeded fixture itself (row counts, event counts per execution, and the
resulting `history_bytes` total) reproduces byte-for-byte on every run.
**Downstream measurements do not**: `EXPLAIN ANALYZE` embeds wall-clock
timings and cache-state-dependent hit/read splits, and the planner's row
*estimates* depend on `ANALYZE`'s statistical sampling (see [the
row-estimate table](#plan) below) — only the buffer *totals* are stable
run to run, per this persona's own admissibility rule. The table below
describes the fixture that is reproducible; it is not a claim about the
`EXPLAIN`/`pg_stat_statements` artifacts:

| share of 1,000 executions | events per execution | role |
|---:|---:|---|
| 5% (`i % 20 == 0`) | 2,001–2,481 | long-running saga tail |
| 20% (`i % 20` in 1..=4) | 202–285 | medium workflow |
| 75% | 16–30 | typical short workflow |

This totals **178,000 events / 80,192,528 bytes (~80.2 MB)** of history for
the target tenant, plus 50 dead-letter rows. A sweep (`NOISE_SWEEP = [3, 15,
100]`) independently scales *background* tenants sharing the same tables —
other `quota_key`s with a light, uniform history — so total `harvest_events`
table size lands at roughly 205k / 313k / 1.08M rows while the target
tenant's own footprint stays fixed. This mirrors how the table actually grows
in production: more tenants accumulate over time. (It is *not* because the
cap keeps one tenant's footprint bounded in real time — `max_history_bytes`
is checked only at admission, never against an already-admitted execution's
ongoing event appends, so a single long-running execution can push a
tenant's aggregate well past its configured cap between admissions; see
[The cost that remains](#the-cost-that-remains-and-why-this-is-not-a-ledger-fix).
The fixture simply holds the target's footprint fixed by construction, to
isolate the effect of the *background* table growing around it.)

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
| 313,000 | `Nested Loop` over `active`, `Index Scan using idx_harvest_events_exec_last`, 1,000 loops | 16,767 (5,254 hit + 11,513 read) |
| 1,078,000 | `Nested Loop` over `active`, `Index Scan using idx_harvest_events_exec_last`, 1,000 loops | 16,769 (10,355 hit + 6,414 read) |

Full captured plans: `docs/perf-artifacts/quota-history-bytes-admission/noise_mult-{3,15,100}.explain.txt`.

Two things this table shows:

1. **The planner already switches plans on its own** as the table grows past
   roughly 300k rows — from a full `Seq Scan` to the index-bounded `Nested
   Loop` the doc comment describes. This happens with **zero code change**;
   `quota_usage_query()`'s `WHERE e.workflow_exec_id IN (SELECT id FROM
   active)` is left exactly as it ships.
2. **Once on the `Nested Loop` plan, buffer cost is flat** across a 3.4x
   growth in total table size (313k → 1,078,000 rows: 16,767 → 16,769
   buffers, effectively unchanged). This confirms the query, on its intended
   plan, costs what the target tenant's own active footprint costs — not
   what the whole table costs.

The row-estimate/actual mismatch also grows as background noise grows —
worth citing as a secondary, uninvestigated finding, not this page's main
subject:

| total rows | estimated rows for `history_bytes`'s join | actual rows | ratio |
|---:|---:|---:|---:|
| 205,000 | 53,890 | 178,000 | 3.3x under |
| 313,000 | 22,318 | 178,000 | 8.0x under |
| 1,078,000 | 15,200 | 178,000 | 11.7x under |

These are read directly off the committed
`noise_mult-{3,15,100}.explain.txt` plans and will drift slightly on a
re-run — unlike the buffer counts, the planner's row estimate depends on
`ANALYZE`'s statistical sampling, which is not deterministic even against
byte-for-byte identical seeded data. The underestimate does not change which
plan is chosen at the sizes measured, but it is the kind of drift that could
tip a borderline choice the wrong way at a size not tested here; extended
statistics on `harvest_events.workflow_exec_id` were not investigated.

## Hypothesis

Below the crossover, the query does a full-table `Seq Scan` because the
planner correctly estimates that scanning the whole (small) table
sequentially is cheaper than 1,000 separate index probes. **Is that scan
actually the more expensive choice once the table is realistically large in
production, or does the planner keep making the right call?** Measured: the
planner keeps making the right call — the crossover already happens well
below typical production table sizes (300k rows is a modest, early-life
`harvest_events` table), and the query's cost is flat across the 3.4x growth
actually measured (313k → 1,078,000 rows). That is not evidence the cost
stays flat at every larger scale: only two points on the `Nested Loop` plan
were tested, and index traversal/cache costs can still grow as an index
itself grows much larger than either fixture here — untested beyond 1.08M
rows.

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
| `LATERAL` rewrite | `Nested Loop` → `Bitmap Heap Scan` via `idx_harvest_events_exec`, 1,000 loops | 15,764 (15,725 hit + 39 read) |

The rewrite **costs 24% more**, not less. Postgres's per-row plan for the
correlated form is a `Bitmap Heap Scan` (build a bitmap, sort, then fetch) —
more expensive per probe at this row count (~178 rows/execution) than either
the unmodified query's own sequential scan at this size, or the plain
`Index Scan` the *unmodified* query reaches on its own once the table is
larger (see the Plan table above: 16,767–16,769 buffers on a table 1.5x–5.3x
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
| `EXPLAIN` buffers @ 313,000 total rows (Nested Loop) | 16,767 |
| `EXPLAIN` buffers @ 1,078,000 total rows (Nested Loop) | 16,769 |
| `pg_stat_statements`, 20 real `load_quota_usage()` calls @ 1,078,000 rows | 335,380 total buffers (16,769/call), mean 52.3ms |
| `LATERAL` rewrite @ 205,000 total rows | 15,764 (vs. 12,745 unmodified — **worse**) |

Tool used for every row: `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` and
`pg_stat_statements`, both against a fresh, fully-migrated database
(`autumn_harvest::full_migrations_sql()`), not a hand-built schema subset.
Full artifacts: `docs/perf-artifacts/quota-history-bytes-admission/`.

**Harness note.** The first captured `pg_stat_statements` snapshot scoped
its `pg_stat_statements_reset()` call to the current database's `dbid` but
not the follow-up `SELECT` — on a shared cluster, `pg_stat_statements`
aggregates per `(dbid, queryid)`, so the unscoped `SELECT` also returned
other databases' (including stale, already-dropped ephemeral benchmark
databases') rows for the identical query text. Caught in review before
merge: the first artifact showed five distinct `calls=20` rows for what
this run drives as exactly one, and the reported top-buffer row
(362,223 total / 18,111 per call) was in fact a leftover row from an
earlier ephemeral database, not this run's. Fixed by scoping the `SELECT`
to the current `dbid` too and asserting exactly one matching row with
`calls` equal to the exact iteration count — the corrected number above
(335,380 / 16,769 per call) now lands almost exactly on the `EXPLAIN`
figure at the same table size, as it should.

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
~16,700–16,800 buffers and ~50–100ms **per admission**, and that cost is
proportional to the target tenant's own accumulated active-execution
history — recomputed from scratch, synchronously, inside the admission
transaction, on *every single* fresh start and spawned child for that
tenant, **for any tenant whose workflow type declares any `QuotaPolicy`
cap and whose admission resolves a quota key** (see [Workload](#workload)
above — the trigger is `has_any_cap()` plus a resolved key, not specifically
`max_history_bytes`). A tenant sitting anywhere near a
configured `max_history_bytes` cap — exactly the tenant that specific cap
exists to protect against — pays the most (its own footprint is largest by
construction), but a tenant whose *only* declared cap is
`max_active_executions` or `max_dead_letters` pays the identical query on
every admission too, for a counter that never even factors into its
admit/reject decision. Either way, this is paid on every admission attempt,
forever, and the bill grows monotonically with that tenant's own footprint.
There is currently no upper bound on this per-admission cost.

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
