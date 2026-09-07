# Pre-registration: does `queue_name = ANY($1)` over several queues, not
# backlog depth or the `id` tiebreak, explain ledger #5's Seq-Scan-and-Sort
# discrepancy against issue #1177's own reported baseline?

**Status:** pre-registration, committed before any measurement in this
assay's apparatus. This is a narrow re-charter of one specific open pit
left by [ledger #5](../assays/0005-claim-batched-seek-and-refine.md):
*"a follow-up check ruled out this assay's own `id` tiebreak as the cause,
leaving an unresolved discrepancy against `docs/performance.md`'s own
#1177 baseline"*.

## 🎯 Question

`docs/performance.md` (issue #1177 section, "Multiple queues: partially
controlled for, not fully") already names an uncontrolled variable and
says so in its own text:

> `idx_harvest_tq_poll` leads with `queue_name`; for `queue_name = ANY($1)`
> over several values, its output is grouped by queue rather than
> necessarily a single global `priority`/`scheduled_at` order, and merging
> those groups can itself require a `Sort` — independent of any residual
> predicate. [...] What remains unconfirmed: issue #1177's own text does
> not say how many queue names `$1` actually held (its fixture description
> mentions rows seeded into a single `default` queue [...]). [...] Whether
> the ten-predicate finding transfers to a genuinely multi-queue bind has
> not been checked here.

Ledger #5's own apparatus queried `queue_name = ANY(ARRAY['bench-q-0',
'bench-q-1','bench-q-2','bench-q-3'])` — 4 queues, matching this page's
`Scenario.queues` default, not #1177's single-queue fixture — and found a
`Seq Scan` + `Sort` even with the residual predicate and the `id` tiebreak
both removed (`forced_index_no_tiebreak_diagnostic.sql`, byte-for-byte
#1177's own baseline `ORDER BY`), under the same `enable_seqscan`/
`enable_bitmapscan = off` bias #1177 used. That is the "unresolved
discrepancy": #1177 reports a clean `Index Scan`, no `Sort`, for this exact
shape; ledger #5's apparatus does not.

**Falsifiable question:** holding backlog depth (10,000 matching `PENDING`
rows), the index bias (`enable_seqscan = off; enable_bitmapscan = off`,
session-local, inside a rolled-back transaction), and the query text
otherwise identical to `forced_index_no_tiebreak_diagnostic.sql`
(`queue_name = <predicate>, state = 'PENDING', scheduled_at <= NOW()`,
`ORDER BY priority DESC, scheduled_at ASC`, `FOR UPDATE SKIP LOCKED`) —
does replacing the 4-queue `queue_name = ANY(ARRAY[...])` predicate with a
single-queue `queue_name = 'bench-q-0'` scalar equality, over an otherwise
identical 10,000-row backlog reseeded entirely into that one queue, reproduce
issue #1177's reported plan shape: `Index Scan using idx_harvest_tq_poll`
with **zero** `Sort` nodes anywhere in the plan?

## Decision this feeds, and who decides

Whoever next re-charters issue #1340's seek-and-refine work (ledger #5's
own named successor pit) needs to know, before designing that fixture,
whether queue count is an independent axis their apparatus must vary, or
whether ledger #5's Seq-Scan-based cost numbers already represent the
general case regardless of queue count. **Decider:** the implementing team
that picks up issue #1340 next — same decider ledger #5 and the
Keystone redis-queue-worker-integration deferral record both name for that
follow-on work. This record does not itself decide whether to build
seek-and-refine; it scopes what that fixture needs to control for.

## ⚖️ Pre-registered criteria

- **Confirms the hypothesis (multi-queue `ANY()` is the explanation):**
  the single-queue run's `EXPLAIN` output contains `Index Scan using
  idx_harvest_tq_poll` and contains **zero** occurrences of a `Sort` node.
- **Refutes the hypothesis:** the single-queue run's `EXPLAIN` output still
  contains a `Sort` node (whether or not it also contains a `Seq Scan`).
  There is no partial-credit reading — a `Sort` node anywhere in the plan
  is a miss, matching how ledger #5 itself read "Seq Scan not Index Scan"
  as a binary presence/absence question.
- These lines do not move once the run exists. A result that lands
  ambiguously (e.g. a `Bitmap Heap Scan` instead of either named shape) is
  reported as **undetermined**, not folded into either verdict.

## Conditions

- Same Postgres 16 instance/version family as ledger #5's own apparatus
  (measured here: PostgreSQL 16.13).
- Same `schema.sql` (`harvest_task_queue`, `idx_harvest_tq_poll` exactly as
  migrated in `20260409000000_harvest_initial/up.sql`), unmodified.
- Backlog depth held constant at 10,000 total `PENDING` rows, `priority`
  and `scheduled_at` values held identical to ledger #5's `seed.sql`
  generation (same `generate_series`, same literal `priority = 0`,
  `scheduled_at = NOW() - INTERVAL '1 second'` for every row) — the only
  seed change is `queues = 1` instead of `queues = 4`, so all 10,000 rows
  land in `bench-q-0` instead of being spread round-robin across four.
  `running_rows = 0` (no concurrency-gate rows; this diagnostic isolates
  the `ORDER BY`/`LIMIT` question only, matching
  `forced_index_no_tiebreak_diagnostic.sql`'s own scope).
- No residual predicate beyond the base `state = 'PENDING' AND
  scheduled_at <= NOW()` and the queue predicate itself — byte-for-byte
  `forced_index_no_tiebreak_diagnostic.sql`'s query otherwise, `id` tiebreak
  already excluded per that file's own round-2 finding.
- `ANALYZE` run after seeding, before measurement, matching ledger #5's own
  `seed.sql`.

## Riskiest assumption, attacked first

That queue count, not backlog depth or apparatus drift, explains the
discrepancy is the cheapest possible explanation to test directly (one
seed-parameter change, one diagnostic query, no code change) and is named
explicitly in `docs/performance.md`'s own text as the untested variable.
If this comes back **refuted**, the discrepancy stays open and is a more
expensive problem — potentially a genuine difference between this
apparatus's Postgres instance/version and #1177's, or a planner-version
behavior change, neither of which this record's cheap test can distinguish
between on its own; that would need to be a separate, further re-charter.

## Time box

Same session; well under a day. This is a single-parameter reseed plus one
`EXPLAIN`, reusing ledger #5's already-archived schema and query text
verbatim.

## Containment

Local, non-production Postgres 16 database (`prospect_assay6`), no
migration added to `autumn-harvest/migrations/`, no crate code touched.
Apparatus archived under `docs/assays/apparatus/0006-...` per the standing
convention; prototype does not merge.

## Prior art already checked

`docs/performance.md`'s own "Multiple queues: partially controlled for,
not fully" section (already quoted above) and ledger #5's report
(post-review round 2's `forced_index_no_tiebreak_diagnostic.sql` and its
surrounding discussion) are the entirety of the prior art — this record's
question is those two documents' own named gap, not a new hypothesis. No
further literature search applies; this is an internal-fixture-control
question, not a Postgres-planner-literature question.
