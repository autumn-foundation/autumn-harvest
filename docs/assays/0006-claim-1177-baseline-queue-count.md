# ⛏️ Prospect: does multi-queue `ANY($1)` explain ledger #5's #1177-baseline discrepancy? (confirmed, per its own literal registered intervention: 0 vs 3 Sort-node lines — see ledger #7 for the production-representative follow-up)

## 🎯 Question

[Ledger #5](0005-claim-batched-seek-and-refine.md) left one named pit open:
its `forced_index_no_tiebreak_diagnostic.sql` — byte-for-byte issue #1177's
own no-residual-predicate baseline query, biased toward
`idx_harvest_tq_poll` with `enable_seqscan`/`enable_bitmapscan = off` —
still showed a `Sort` node over a full scan of the matching backlog, where
#1177 itself reports a clean `Index Scan`, no `Sort`. `docs/performance.md`
already names one variable that reproduction didn't hold constant against
#1177's own fixture: `queue_name = ANY($1)` over several queues, versus
#1177's single-queue seed.

**Falsifiable question, exactly as pre-registered:** holding backlog depth
(10,000 matching rows), index bias, and query text otherwise identical,
does replacing the 4-queue `queue_name = ANY(ARRAY[...])` predicate with a
single-queue `queue_name = 'bench-q-0'` **scalar equality** reproduce issue
#1177's reported plan shape — `Index Scan using idx_harvest_tq_poll`,
**zero** `Sort` nodes?

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
(commit `fffeef6`). Its own text (lines 38-48) defines the single-queue
intervention explicitly as scalar equality, `queue_name = 'bench-q-0'` —
not `ANY()` over a single-element array.

- **Confirms:** single-queue run's `EXPLAIN` contains `Index Scan using
  idx_harvest_tq_poll` and zero `Sort` nodes.
- **Refutes:** single-queue run still shows a `Sort` node anywhere in the
  plan.
- No partial credit; an ambiguous shape (e.g. `Bitmap Heap Scan`) reports
  as undetermined.

## ⚠️ Post-review corrections and a self-correction (Codex, two rounds)

**Round 1 (P2, legitimate, fixed in place):** the first pass's custom
`seed_single_queue.sql` omitted `concurrency_key`/`concurrency_cap`, while
ledger #5's own `seed.sql` (reused for the multi-queue control) populates
both on every row — different tuple width, so the original 591-vs-53-buffer
comparison wasn't from otherwise-identical row shapes. Fixed by deleting
the custom seed and reusing `../0005-claim-batched-seek-and-refine/seed.sql`
directly for every arm, varying only its `queues` parameter. Re-running
the **pre-registered scalar-equality intervention** with the corrected
seed still shows zero `Sort` nodes (53 buffers) — the fix changes the
measurement's rigor, not its verdict.

**Round 1 (P1) and this assay's own overcorrection, since reverted:** a
second finding (verified against `autumn-harvest/src/queue.rs:641`) noted
that scalar equality is not a shape any production code path takes — the
claim path always binds `queue_name = ANY($2)`, even for a single-queue
worker. That is true and important, but this assay's first response to it
was wrong: it substituted an `ANY(ARRAY['bench-q-0'])` arm for the
pre-registered scalar-equality one, found a `Sort` node there, and used
*that* result to flip this assay's own verdict to "refuted." **A second
Codex review round (P1, round 2) caught the error**: the committed
pre-registration's own text defines "single-queue" as scalar equality, and
that intervention passed its own stated criterion — grading a different,
unregistered intervention instead and reporting a reversed verdict is
exactly the after-the-fact goalpost-moving this ledger's own charter
forbids ("a discovery... spawns an explicitly re-chartered assay — it
never edits this one").

**This report is the correction of that overcorrection.** The verdict
below is graded against the pre-registration's own literal, committed
criteria — scalar equality — and nothing else. The `ANY()`-cardinality
question is real, was worth asking, and now has its own proper
pre-registration and its own report:
[ledger #7](0007-claim-any-cardinality.md). Its finding does **not**
change this assay's verdict; it answers a different, explicitly
re-chartered question.

## 🔍 Prior art

`docs/performance.md`'s own "Multiple queues: partially controlled for,
not fully" section and ledger #5's post-review round 2 are the entire
prior art for the original framing; this question is those two documents'
own named, explicitly unconfirmed gap, not a new hypothesis invented here.
No external literature applies — this is a fixture-control question
internal to this repo's own prior measurements.

## 🧪 Apparatus

`docs/assays/apparatus/0006-claim-1177-baseline-queue-count/`:

- `schema.sql` — ledger #5's own `harvest_task_queue` + `idx_harvest_tq_poll`,
  copied unmodified.
- `single_queue_diagnostic.sql` — **the pre-registered test.** Ledger #5's
  `forced_index_no_tiebreak_diagnostic.sql`, with the queue predicate
  changed to scalar equality (`queue_name = 'bench-q-0'`). Everything
  else — the index bias, the base predicate, the `ORDER BY` (no `id`
  tiebreak), `LIMIT 50`, `FOR UPDATE SKIP LOCKED` — copied verbatim.
- `single_queue_any_diagnostic.sql` — the arm this assay does **not**
  grade (see post-review above); its own question and finding live in
  [ledger #7](0007-claim-any-cardinality.md).
- `multi_queue_control.sql` — the control: ledger #5's own
  `forced_index_no_tiebreak_diagnostic.sql` re-run unmodified (4-queue
  `ANY`), to confirm the phenomenon reproduces on *this* Postgres instance
  (16.13) before trusting any comparison against it.
- `run_assay.sh` — seeds every arm from ledger #5's own `seed.sql`
  (`queues` is the only varied parameter) and runs all three diagnostics
  in one pass; idempotent (`schema.sql` has `DROP TABLE IF EXISTS`,
  matching ledger #5's own fix).

**Stubs list:** none beyond what ledger #5 already declared for this exact
query shape (no `worker_id`/heartbeat columns needed, no concurrency-gate
predicate, no real client — this is a single `EXPLAIN` diagnostic, same
scope as `forced_index_no_tiebreak_diagnostic.sql` itself).

## 📊 Assay

All three arms seed from the identical `seed.sql` (only `queues` varies),
same `enable_seqscan`/`enable_bitmapscan = off` bias, same rolled-back
transaction, same `LIMIT 50`, same `FOR UPDATE SKIP LOCKED`.

**Control** (4-queue `ANY`, 10,000 rows spread round-robin across
`bench-q-0..3`): `Sort` node present, `Index Scan` reads all 10,000
matching rows, 591 buffers, 5.075ms (execution time; see the note below
the numbers on why buffers, not this, are what's graded).

**Pre-registered test** (single-queue scalar equality, all 10,000 rows in
`bench-q-0`): **no `Sort` node**, bounded scan (50 rows read, matching
`LIMIT`), 53 buffers, 0.171ms.

**Not graded here — see ledger #7** (single-queue `ANY` over a
single-element array): `Sort` node present, full 10,000-row scan, 195
buffers.

Buffer counts (591 / 53 / 195) are bit-for-bit identical across every
rerun of this apparatus, including the final rerun that produced the
numbers above; execution-time milliseconds are not (they moved run to
run — e.g. this control read 4.489-5.075ms across different runs on
identical data) and are reported only as the archived run's own number,
never as a claim about relative cost. Buffers are what this report's
verdict is graded against.

`grep -c "Sort Key"`: control 1, pre-registered scalar test 0, `ANY`
arm (ledger #7's own evidence) 1. Full output archived at
[`results/multi_queue_control.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/multi_queue_control.explain.txt),
[`results/single_queue_scalar.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/single_queue_scalar.explain.txt),
[`results/single_queue_any.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/single_queue_any.explain.txt).

**Against the line:** the pre-registered single-queue run (scalar
equality) shows `Index Scan using idx_harvest_tq_poll`, zero `Sort`
nodes — **confirms** the pre-registered hypothesis exactly, with no
ambiguity to adjudicate.

## 🏁 Verdict

**Confirmed**, graded strictly against this assay's own committed,
literal criteria: swapping the 4-queue `ANY()` predicate for a
single-queue scalar-equality predicate — the intervention the
pre-registration actually specified — eliminates the `Sort` node and
restores bounded `LIMIT` pushdown (53 vs. 591 buffers, at identical
backlog depth and identical row shape).

**What this resolves, and what it doesn't — read narrowly.** This assay
establishes that *some* single-value queue predicate gets the cheap plan
where a 4-value `ANY()` predicate doesn't; it does **not** establish that
this is because of *cardinality* per se, and it does **not** establish
that a real single-queue deployment gets this plan, since
`autumn-harvest/src/queue.rs:641` never emits scalar equality — every
deployment, one queue or many, binds `ANY($2)`. That distinction is
exactly what [ledger #7](0007-claim-any-cardinality.md) was chartered to
resolve, and its answer is a `Sort` node persists under `ANY()` even at
cardinality 1 — so this assay's confirmed result, while accurate to its
own letter, should not be read as "single-queue deployments are cheap."
It answers the narrower question it actually pre-registered: the discrete
identity of the 4-queue `ANY()` predicate, not queue count in the
abstract, is what a scalar-equality control lacks.

**For the named decider:** do not use this report alone to conclude
single-queue deployments avoid this cost — read it together with ledger
#7, whose finding is the operationally relevant one for any real
deployment shape.

## 💰 Cost to productionize

Not applicable — this assay answers a diagnostic question about an
existing, already-cited defect, not a feature to build. No stub carries
forward into a build estimate.

## 🔬 Reproduce

```sh
sudo -u postgres createdb prospect_assay6   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0006-claim-1177-baseline-queue-count
sudo -u postgres PGDATABASE=prospect_assay6 ./run_assay.sh
grep -c "Sort Key" results/multi_queue_control.explain.txt   # 1
grep -c "Sort Key" results/single_queue_scalar.explain.txt   # 0 -- this assay's own line
grep -c "Sort Key" results/single_queue_any.explain.txt      # 1 -- ledger #7's evidence, not graded here
```

`schema.sql`, `single_queue_diagnostic.sql`, `single_queue_any_diagnostic.sql`,
`multi_queue_control.sql`, `run_assay.sh`, and the full `results/*.txt` /
`results/run.log` this report draws from are archived alongside this
report. No migration was added to `autumn-harvest/migrations/`; no crate
code changed. The prototype does not merge.
