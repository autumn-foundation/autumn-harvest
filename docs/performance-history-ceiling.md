# Workflow-history-ceiling scanner: correlated event-count subquery fixed (issue #493)

`timeout::enforce_workflow_history_ceiling` -- the scanner that fails a
RUNNING workflow execution once its durable `harvest_events` row count
reaches an operator-configured ceiling (`HarvestBuilder::max_workflow_history_events`)
-- ran its scanner query with the same event count computed twice per RUNNING
row: once (as a correlated subquery) to filter, and again (a second,
independent evaluation of the identical subquery) to report the value it just
filtered on. This page measures that cost and fixes it.

## 🎯 Workload

`enforce_workflow_history_ceiling` runs once per timeout-scanner tick
(`timeout::enforce_timeouts_once`), whenever an operator has configured a
history ceiling, against **every** RUNNING workflow execution in the shard --
not scoped to any one workflow or request. It exists to catch runaway/looping
workflows before their event history grows without bound; the population it
scans over-represents exactly that failure mode, since a hung workflow stays
RUNNING (and keeps accumulating events) long after a healthy one has already
completed and left the RUNNING set.

`tests/integration/history_ceiling_claim_tests.rs::zz_capture_history_ceiling_claim_evidence`
seeds a production-shaped fixture directly (not the claim-path's
`claim_bench_support` harness, which drives a different query): 100,000
`harvest_workflow_executions` rows, 3,000 (3%) of them RUNNING -- a plausible
steady-state fraction for a busy fleet where most work finishes quickly --
and roughly 4,000,000 `harvest_events` rows spread across them with a
skewed, per-state event-count distribution. RUNNING rows get a heavier tail
than non-RUNNING ones (10% land in a 1,000-8,000 event band straddling the
5,000-event ceiling used throughout this page, vs. 1% capped at 3,000 for
non-RUNNING rows) for the reason above: still-running executions
over-represent hung/looping workflows. Reproduce with:

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh
```

## 📈 Profile

This scanner is not on the claim path `docs/performance.md` profiles, and it
runs once per tick against the whole RUNNING set rather than once per
candidate row, so it does not show up in that page's per-gate buffer
ranking. Read directly from `EXPLAIN (ANALYZE, BUFFERS)`: the query is a
single statement with two internal `SubPlan`s over the same correlated
`COUNT(*)` expression, and one of the two -- the `WHERE`-clause filter --
dominates the statement's own cost (see below), since it runs once per
RUNNING row (`loops=3000` in the fixture) while the `SELECT`-list projection
only runs, lazily, for rows that already passed the filter (`loops=36`).

## 🧭 Plan

Before (the `WHERE`-clause `SubPlan`, isolated -- full plan in
[`docs/perf-artifacts/history-ceiling-scanner/before-history-ceiling.explain.txt`](perf-artifacts/history-ceiling-scanner/before-history-ceiling.explain.txt)):

```text
Filter: ((harvest_workflow_executions.state = 'RUNNING'::text) AND ((SubPlan 2) >= '5000'::bigint))
Buffers: shared hit=22673 read=6881 dirtied=5948 written=264
SubPlan 1                                    -- SELECT-list projection (lazy, loops=36)
  ->  Aggregate (actual rows=1 loops=36)
        Buffers: shared hit=4732
SubPlan 2                                    -- WHERE-clause filter (loops=3000, every RUNNING row)
  ->  Aggregate (actual rows=1 loops=3000)
        Buffers: shared hit=14979 read=6850 dirtied=5948 written=263
```

After (full plan in
[`docs/perf-artifacts/history-ceiling-scanner/after-history-ceiling.explain.txt`](perf-artifacts/history-ceiling-scanner/after-history-ceiling.explain.txt)):

```text
CTE Scan on oversized_candidates  (actual rows=36 loops=1)
  Filter: (oversized_candidates.event_count >= '5000'::bigint)
  Buffers: shared hit=21909 read=4
  CTE oversized_candidates
    ->  Bitmap Heap Scan on harvest_workflow_executions (actual rows=3000 loops=1)
          SubPlan 1                          -- computed once per RUNNING row, read twice downstream
            ->  Aggregate (actual rows=1 loops=3000)
                  Buffers: shared hit=21829
```

The changed node: two `SubPlan`s over the identical correlated expression
collapse into one, evaluated inside the `MATERIALIZED` CTE and read (not
re-evaluated) by both the CTE's own `event_count` column and the outer
`WHERE` filter.

## 💡 Hypothesis

The query needs each RUNNING execution's event count in two places: the
`SELECT` list (to report `event_count` to the caller) and the `WHERE` clause
(to filter on it, since a `SELECT`-list alias is not visible to the `WHERE`
clause of the same query -- Postgres resolves `WHERE` before `SELECT`).
Writing the correlated `COUNT(*)` subquery out twice, once at each site,
makes Postgres evaluate it twice per row: once to filter (every RUNNING row,
`loops=3000` in the fixture) and once more, for the rows that already passed
the filter, to project (`loops=36`). The mechanism is structural, not a
missing index: `idx_harvest_events_exec_last` already serves each
evaluation as an index lookup, so this is pure duplicated work, not a scan
inefficiency.

A plain derived-table wrap was tried and rejected as a fix, confirmed by
`EXPLAIN` rather than assumed:

```sql
SELECT * FROM (
    SELECT ..., (SELECT COUNT(*) FROM harvest_events WHERE ...) AS event_count
    FROM harvest_workflow_executions WHERE state = 'RUNNING'
) sub WHERE sub.event_count >= $1
```

Postgres pulls a plain subquery up into the outer query during planning and
re-duplicates the correlated expression at each reference site, reproducing
the identical two-`SubPlan` shape byte-for-byte (verified directly:
`shared hit=28370` total, both `SubPlan 1` (47 loops) and `SubPlan 2` (3000
loops) both present, on an earlier exploratory fixture). A `MATERIALIZED`
CTE opts out of that pull-up -- it is a planner *barrier*, not just a
naming device -- so the count is computed exactly once per RUNNING row and
both the `SELECT` list and the `WHERE` clause read the same materialized
column.

## 🔧 Change

`autumn-harvest/src/timeout.rs`: `workflow_history_ceiling_query()` (already
extracted from `enforce_workflow_history_ceiling`'s inline literal by a
separate harness-only commit) rewritten from the double-subquery form to:

```sql
WITH oversized_candidates AS MATERIALIZED (
    SELECT id, workflow_id, workflow_name, queue_name, parent_id, parent_close_policy,
        (SELECT COUNT(*) FROM harvest_events
         WHERE workflow_exec_id = harvest_workflow_executions.id)::bigint AS event_count
    FROM harvest_workflow_executions
    WHERE state = 'RUNNING')
SELECT id, workflow_id, workflow_name, queue_name, parent_id, parent_close_policy, event_count
FROM oversized_candidates
WHERE event_count >= $1
```

No migration, no new index, no lock beyond the query's own (unchanged)
read -- this is a pure query-text rewrite behind an existing function
signature. `enforce_workflow_history_ceiling`'s Rust body, the transaction
it opens per oversized row, and every downstream side effect (event append,
state transition, task cancellation, parent-close cascade, completion
triggers) are untouched.

Two unit tests in `timeout.rs` pin the shape: `workflow_history_ceiling_query_references_correct_columns`
(the pre-existing column/table checks) and the new
`workflow_history_ceiling_query_counts_events_exactly_once_per_row`, which
asserts the correlated `SELECT COUNT(*) FROM harvest_events` text appears
**exactly once** and that the CTE is `MATERIALIZED` -- so a future edit that
reintroduces the duplicate-evaluation shape fails a fast, DB-free test
before it ever reaches a benchmark.

## 📊 Measurement

One cold claim (`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)`),
100,000-execution / 3,000-RUNNING / ~4,000,000-event fixture, ceiling=5,000,
before and after captured **in the same test run against the same seeded
fixture** (not two separate runs) so the comparison is not confounded by
fixture-generation variance between runs -- the methodology
`docs/performance.md` itself flags as a real source of noise on this class
of measurement. Full artifacts (`EXPLAIN`, `pg_stat_statements`, and the
sorted result-row list for both forms) are committed under
[`docs/perf-artifacts/history-ceiling-scanner/`](perf-artifacts/history-ceiling-scanner/).

| | Buffers (`hit`) | Buffers (`read`) | **Total (`hit+read`)** | `dirtied` | `written` |
|:--|--:|--:|--:|--:|--:|
| Before | 22,673 | 6,881 | **29,554** | 5,948 | 264 |
| After | 21,909 | 4 | **21,913** | 0 | 0 |
| **Δ** | | | **-7,641 (-25.85%)** | -5,948 | -264 |

Tool: `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)`, read
directly from the committed plan files -- `pg_stat_statements` did not
separately corroborate this capture (see
[known limitation](#known-limitation-pg_stat_statements-capture-came-back-empty)
below), so the primary evidence here is the `EXPLAIN` buffer totals alone,
which the admissible-evidence rules treat as sufficient on their own.

**-25.85% clears the impact floor** (`>=20%` reduction in total buffers).
The `dirtied`/`written` reduction to zero is a secondary, uncontracted
signal: the pre-fix query's redundant second pass over
`harvest_events`' index/heap pages set MVCC visibility hint bits a second
time on a freshly bulk-loaded, not-yet-vacuumed fixture, incurring real
buffer writeback; the fixed query touches each page once, so nothing is
dirtied. This is reported for completeness, not claimed as an independent
win -- it is a byproduct of halving the redundant scan, not a separate
mechanism.

Rows read: `Rows Removed by Filter: 2964` in both plans (identical --
2,964 = 3,000 RUNNING rows minus the 36 that clear the ceiling), since both
forms still visit every RUNNING row's event count once each; this fix
removes a **duplicate** evaluation, not a redundant row visit, so it is
correctly reported against the buffers floor rather than the
rows-read floor.

Statement count: unaffected. `enforce_workflow_history_ceiling` issues one
statement per scanner tick regardless of RUNNING population size -- this was
never an N+1 across rows, only duplicated work *within* one statement.

WAL bytes: not applicable -- this is a pure `SELECT`; no new index, so no
write-path cost to measure per the report format's own scoping (the
`written`-buffer reduction above is reclaim/eviction traffic during the read,
not WAL).

## ✅ Equivalence

Both query forms run against the **identical** fixture in the **identical**
test run (`zz_capture_history_ceiling_claim_evidence`), each producing its
sorted `(id, event_count)` result set, asserted equal:

```
equivalence confirmed: before and after agree on all 36 oversized rows
```

Full sorted row lists (UUID + `event_count` for every flagged execution) are
committed as
[`before-result-rows.txt`](perf-artifacts/history-ceiling-scanner/before-result-rows.txt) /
[`after-result-rows.txt`](perf-artifacts/history-ceiling-scanner/after-result-rows.txt)
and are byte-identical.

This is not a case with interesting NULL-semantics or ordering edge cases to
enumerate: `COUNT(*)` never returns `NULL`, the query carries no `ORDER BY`
(the caller iterates the full oversized set, not a `LIMIT`-bounded page, so
there is no tie-breaker to worry about), and the rewrite changes *how many
times* the identical correlated expression is evaluated, not *what* it
computes or *which* rows satisfy `state = 'RUNNING' AND event_count >= $1` --
both forms are the same predicate over the same data, one of them just pays
for it twice.

End-to-end behavioral correctness (not just result-set shape) is covered by
the always-run
`history_ceiling_claim_tests::enforce_workflow_history_ceiling_terminates_only_oversized_running_rows`,
which exercises the real `enforce_workflow_history_ceiling` function against
the fixed query and asserts: a RUNNING row over the ceiling transitions to
`FAILED` with a `history_ceiling_exceeded` error, a RUNNING row **exactly
at** the ceiling also fails (`>=`, not `>`), a RUNNING row one event under
the ceiling is left alone, and a non-RUNNING row is never touched regardless
of its event count.

Isolation/visibility: unchanged -- the fix touches only the read-only
candidate-selection query; the per-row `FOR UPDATE` re-check transaction
inside `enforce_workflow_history_ceiling` (unaffected by this change) is
still what makes each termination race-safe against a concurrent completion
or a duplicate scanner tick.

## 💸 Write cost

None. No index added, no schema change, no write-path statement touched.
The fix is a read-path-only query-text rewrite.

## 🔬 Reproduce

```bash
# Fast, always-run functional correctness test (no seeding, runs against a
# migrated HARVEST_TEST_DATABASE_URL or the container fallback):
cargo test -p autumn-harvest --features db,testing --test integration -- \
  history_ceiling_claim_tests::enforce_workflow_history_ceiling_terminates_only_oversized_running_rows

# Shape-pinning unit tests (no DB):
cargo test -p autumn-harvest --features db --lib timeout::tests::workflow_history_ceiling

# Full evidence capture (seeds ~4M rows; several minutes):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh
```

## Known limitation: `pg_stat_statements` capture came back empty

The capture test resets and re-queries `pg_stat_statements` around each
form's execution, but the LIKE-filtered query against it returned no rows
for either "before" or "after" in every run of this fixture. The most
likely cause: this file runs the query once via `EXPLAIN (ANALYZE, ...)`
followed by one plain execution per form, and `pg_stat_statements` under its
default `track = 'top'` setting does not record a statement wrapped in
`EXPLAIN` as the inner query text -- so only the plain execution should have
been tracked, and it is possible the query planner's parameterized-literal
normalization changed the stored text enough to miss the `LIKE` pattern used
here. This was not chased further: the primary evidence above (`EXPLAIN`
`BUFFERS` totals) is admissible on its own per the evidence rules, and this
scanner runs once per tick rather than in a tight loop, so there is no
"cumulative real-execution" number analogous to the claim path's drained-backlog
measurements to substitute for it. A future pass could investigate the
`pg_stat_statements` miss directly if a cumulative, multi-tick number becomes
useful.

## See also

* `autumn-harvest/src/timeout.rs` -- `workflow_history_ceiling_query()`, the
  query this page measures.
* `tests/integration/history_ceiling_claim_tests.rs` -- the harness and
  evidence-capture test.
* `autumn-harvest/scripts/history_ceiling_claim_perf_repro.sh` -- regenerates
  the committed artifacts from a clean checkout.
* `docs/perf-artifacts/history-ceiling-scanner/` -- committed before/after
  `EXPLAIN` and result-set evidence.
* `docs/performance.md` -- the claim-path measurement page this scanner is
  deliberately *not* part of (it is a separate scanner, not one of
  `claim_task_query()`'s accreted predicates).
