# ⛏️ Prospect: does multi-queue `ANY($1)` explain ledger #5's #1177-baseline discrepancy? (pursue-the-explanation: confirmed, 0 vs 0 Sort nodes)

## 🎯 Question

[Ledger #5](0005-claim-batched-seek-and-refine.md) left one named pit open:
its `forced_index_no_tiebreak_diagnostic.sql` — byte-for-byte issue #1177's
own no-residual-predicate baseline query, biased toward
`idx_harvest_tq_poll` with `enable_seqscan`/`enable_bitmapscan = off` —
still showed a `Sort` node over a full scan of the matching backlog, where
#1177 itself reports a clean `Index Scan`, no `Sort`. `docs/performance.md`
already names the one variable that reproduction didn't hold constant
against #1177's own fixture: `queue_name = ANY($1)` over several queues,
versus #1177's single-queue seed.

**Falsifiable question:** holding backlog depth (10,000 matching rows),
index bias, and query text otherwise identical, does swapping the 4-queue
`queue_name = ANY(ARRAY[...])` predicate for a single-queue
`queue_name = 'bench-q-0'` scalar equality reproduce issue #1177's reported
plan shape — `Index Scan using idx_harvest_tq_poll`, **zero** `Sort` nodes?

**Decision this feeds:** whoever next re-charters issue #1340's
seek-and-refine work (ledger #5's own named successor) needs to know
whether queue count is an independent axis their fixture must vary, or
whether ledger #5's Seq/forced-Index-Scan-plus-Sort cost numbers already
represent the general case. **Decider:** the implementing team that picks
up issue #1340, per ledger #5 and the Keystone redis-worker-integration
deferral record, which both point future claim-path work at the same
owner.

## ⚖️ Pre-registration

Committed before any measurement:
[`docs/rnd/2026-09-07-claim-1177-baseline-queue-count-preregistration.md`](../rnd/2026-09-07-claim-1177-baseline-queue-count-preregistration.md)
(commit `fffeef6`).

- **Confirms:** single-queue run's `EXPLAIN` contains `Index Scan using
  idx_harvest_tq_poll` and zero `Sort` nodes.
- **Refutes:** single-queue run still shows a `Sort` node anywhere in the
  plan.
- No partial credit; an ambiguous shape (e.g. `Bitmap Heap Scan`) reports
  as undetermined. That case did not arise.

## 🔍 Prior art

`docs/performance.md`'s own "Multiple queues: partially controlled for,
not fully" section and ledger #5's post-review round 2 are the entire
prior art; this question is those two documents' own named, explicitly
unconfirmed gap, not a new hypothesis invented here. No external
literature applies — this is a fixture-control question internal to this
repo's own prior measurements.

## 🧪 Apparatus

`docs/assays/apparatus/0006-claim-1177-baseline-queue-count/`:

- `schema.sql` — ledger #5's own `harvest_task_queue` + `idx_harvest_tq_poll`,
  copied unmodified.
- `seed_single_queue.sql` — ledger #5's own `seed.sql` generation, with
  `queues` fixed to 1 (all 10,000 rows land in `bench-q-0`) instead of
  parameterized; otherwise identical row shape (`priority = 0`,
  `scheduled_at = NOW() - INTERVAL '1 second'` for every row).
- `single_queue_diagnostic.sql` — ledger #5's
  `forced_index_no_tiebreak_diagnostic.sql`, with the queue predicate
  changed from `queue_name = ANY(ARRAY[4 values])` to
  `queue_name = 'bench-q-0'`. Everything else — the index bias, the base
  predicate, the `ORDER BY` (no `id` tiebreak), `LIMIT 50`,
  `FOR UPDATE SKIP LOCKED`, the rolled-back transaction — copied verbatim.
- `multi_queue_control.sql` — the control: ledger #5's own
  `forced_index_no_tiebreak_diagnostic.sql` re-run unmodified (via ledger
  #5's own `seed.sql` with `queues=4`), to confirm the phenomenon
  reproduces on *this* Postgres instance (16.13) before trusting the
  single-queue comparison against it.
- `run_assay.sh` — seeds and runs both in one pass; idempotent (schema.sql
  has `DROP TABLE IF EXISTS`, matching ledger #5's own fix).

**Stubs list:** none beyond what ledger #5 already declared for this exact
query shape (no `worker_id`/heartbeat columns needed, no concurrency-gate
predicate, no real client — this is a single `EXPLAIN` diagnostic, same
scope as `forced_index_no_tiebreak_diagnostic.sql` itself).

## 📊 Assay

**Control** (4-queue `ANY`, 10,000 rows spread round-robin across
`bench-q-0..3`, same bias):

```
Limit (actual rows=50 loops=1)             Buffers: shared hit=591
  LockRows (actual rows=50 loops=1)
    Sort (actual rows=50 loops=1)          Sort Key: priority DESC, scheduled_at
      Index Scan using idx_harvest_tq_poll (actual rows=10000 loops=1)
      Index Cond: queue_name = ANY('{bench-q-0,bench-q-1,bench-q-2,bench-q-3}'::text[]) ...
```

Execution time 4.489ms. The scan itself is an `Index Scan` here (not a
`Seq Scan` — ledger #5's own natural-planner run without the bias showed
`Seq Scan`; this run has the same bias ledger #5's *forced* diagnostic
used), but it still reads all 10,000 matching rows and still needs a
`Sort` on top — the phenomenon ledger #5 flagged (a `Sort` node present,
no `LIMIT` pushdown) reproduces on this instance. Full output:
[`results/multi_queue_control.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/multi_queue_control.explain.txt).

**Test** (single-queue scalar equality, same 10,000 total rows, all in
`bench-q-0`, same bias):

```
Limit (actual rows=50 loops=1)             Buffers: shared hit=53
  LockRows (actual rows=50 loops=1)
    Index Scan using idx_harvest_tq_poll (actual rows=50 loops=1)
    Index Cond: queue_name = 'bench-q-0' ...
```

Execution time 0.099ms. **No `Sort` node anywhere in the plan** — `grep -c
Sort` returns 0 against 3 for the control. The `Index Scan` itself reads
only 50 rows (`actual rows=50`, matching `LIMIT`), not the full 10,000 —
genuine bounded seek, not a full scan that happens to skip an explicit
sort step. 591 buffers (control) vs. 53 (test) at identical backlog depth.
Full output:
[`results/single_queue.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/single_queue.explain.txt).

**Against the line:** single-queue run shows `Index Scan using
idx_harvest_tq_poll`, zero `Sort` nodes — **confirms** the pre-registered
hypothesis exactly, with no ambiguity to adjudicate.

## 🏁 Verdict

**Confirmed** (this is a mechanism-explanation assay, not a
pursue/kill-a-feature one — see below for what "confirmed" resolves and
does not).

The `Sort` node ledger #5 flagged as an "unresolved discrepancy against
`docs/performance.md`'s own #1177 baseline" is explained by the multi-queue
`queue_name = ANY($1)` binding, not by backlog depth (held constant at
10,000 rows both ways), the `id` tiebreak (already absent from both
queries), or apparatus/Postgres-version drift (the control reproduces the
phenomenon on this same instance). `idx_harvest_tq_poll` leads with
`queue_name`; a single-value equality lets the index return rows in
already-globally-ordered form, so the planner elides the sort and pushes
`LIMIT` into the scan (50 rows read, not 10,000). An `ANY()` bind over
several values makes the index's output ordered *within* each queue's
segment but not globally across segments, so a `Sort` is genuinely needed
to merge them — independent of any residual predicate, exactly as
`docs/performance.md`'s own text predicted before this assay ran.

**What this resolves:** the specific "why does my apparatus disagree with
#1177" question ledger #5 left open. It is not itself evidence that
#1177's ten-predicate collapse *doesn't* generalize to multi-queue binds —
that remains a separate, unresolved question `docs/performance.md` also
flags ("whether the ten-predicate finding transfers to a genuinely
multi-queue bind has not been checked here"), and this assay didn't test
it either (no residual predicate was reintroduced here). What it does
settle: ledger #5's own batched-seek cost numbers, and any future
seek-and-refine re-charter of issue #1340, are measuring the multi-queue
case specifically — a genuinely harder baseline than #1177's own
single-queue one — not an apparatus bug or an artifact of table depth.

**For the named decider:** a future #1340 re-charter's fixture needs queue
count as an explicit, independently-varied axis. A single-queue deployment
gets the cheap bounded-seek plan for free, with no batching or
seek-and-refine work needed at all, for this specific `ORDER BY`/`LIMIT`
question (residual predicates like the sticky `CASE` and the concurrency
gate are untouched by this finding and remain their own cost). A
multi-queue deployment does not, and that's the shape ledger #5's own
numbers already describe correctly — they were never wrong, just
unlabeled as multi-queue-specific.

## 💰 Cost to productionize

Not applicable — this assay answers a diagnostic question about an
existing, already-cited defect, not a feature to build. No stub carries
forward into a build estimate.

## 🔬 Reproduce

```sh
sudo -u postgres createdb prospect_assay6   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0006-claim-1177-baseline-queue-count
sudo -u postgres PGDATABASE=prospect_assay6 ./run_assay.sh
grep -c Sort results/multi_queue_control.explain.txt   # 3
grep -c Sort results/single_queue.explain.txt           # 0
```

`schema.sql`, `seed_single_queue.sql`, `single_queue_diagnostic.sql`,
`multi_queue_control.sql`, `run_assay.sh`, and the full `results/*.txt` /
`results/run.log` this report draws from are archived alongside this
report. No migration was added to `autumn-harvest/migrations/`; no crate
code changed. The prototype does not merge.
