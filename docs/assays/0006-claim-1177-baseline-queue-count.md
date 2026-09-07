# ⛏️ Prospect: does multi-queue `ANY($1)` explain ledger #5's #1177-baseline discrepancy? (refuted: Sort node persists at cardinality 1, 195 buffers, `ANY` vs 53 buffers, `=`)

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

**Falsifiable question:** holding backlog depth (10,000 matching rows),
index bias, and query text otherwise identical, does reducing ledger #5's
apparatus from 4 queues to 1 — keeping the production-representative
`queue_name = ANY($2)` binding shape (`autumn-harvest/src/queue.rs:641`
always binds this way, never a scalar equality, regardless of how many
queues a worker polls) — reproduce issue #1177's reported plan shape:
`Index Scan using idx_harvest_tq_poll`, **zero** `Sort` nodes?

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
  as undetermined.

## ⚠️ Post-review corrections (Codex)

This assay's first pass (commit `7dcf677`) operationalized "single-queue"
as `queue_name = 'bench-q-0'` — scalar equality — and reported a
**confirmed** verdict on that basis. Two review findings on the PR caught
real defects in that pass, both verified against the codebase before this
report was rewritten:

1. **P1 — the scalar-equality test isn't production-representative.** The
   claim path's actual query (`autumn-harvest/src/queue.rs:641`,
   confirmed by reading the file) binds `queue_name = ANY($2)`
   unconditionally — there is no code path where a single-queue worker
   gets scalar equality instead. The original test therefore showed only
   that scalar equality is cheap (never in question) and said nothing
   about what a real single-queue deployment's plan looks like. Fixed by
   adding `single_queue_any_diagnostic.sql`: the same query, same bias,
   `ANY(ARRAY['bench-q-0'])` — a single-element array, the actual shape a
   one-queue worker binds. The scalar-equality file is kept only as a
   secondary sanity check, relabeled as such, not as this assay's
   evidence.
2. **P2 — the original single-queue seed wasn't row-identical to the
   control's.** The custom `seed_single_queue.sql` omitted
   `concurrency_key`/`concurrency_cap`, while ledger #5's own `seed.sql`
   (reused unmodified for the multi-queue control) populates both on every
   row — different tuple width, so the reported 591-vs-53-buffer
   comparison wasn't from otherwise-identical row shapes. Fixed by
   deleting the custom seed and reusing `../0005-claim-batched-seek-and-refine/seed.sql`
   directly for every arm, varying only its `queues` parameter (4 for the
   control, 1 for both single-queue arms; `keys=256`, `running_rows=0`
   unchanged throughout).

Rerunning with both fixes **reverses the verdict** — see below. This is
exactly the "genuinely learned the line was miscalibrated" case the
pre-registration's own terms anticipate: the committed pass/fail criteria
did not move, but what "single-queue" means was corrected to the shape
that actually answers the question, following the same in-place
post-review-correction pattern ledger #3/#4/#5 established for apparatus
defects a reviewer catches before the report is trusted.

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
- `single_queue_any_diagnostic.sql` — **the corrected, production-representative
  test.** Ledger #5's `forced_index_no_tiebreak_diagnostic.sql`, with the
  queue predicate changed to `queue_name = ANY(ARRAY['bench-q-0'])` — a
  single-element array, varying only cardinality against the control's
  4-element array, matching the operator shape `queue.rs` actually uses.
- `single_queue_diagnostic.sql` — secondary arm, scalar equality
  (`queue_name = 'bench-q-0'`); kept as a sanity check, not evidence for
  this assay's verdict (see post-review §1 above).
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
`bench-q-0..3`):

```
Limit (actual rows=50)         Buffers: shared hit=591   Execution Time: 4.391ms
  LockRows (actual rows=50)
    Sort (actual rows=50)      Sort Key: priority DESC, scheduled_at
      Index Scan using idx_harvest_tq_poll (actual rows=10000)
      Index Cond: queue_name = ANY('{bench-q-0,bench-q-1,bench-q-2,bench-q-3}'::text[]) ...
```

**Test** (single-queue `ANY`, all 10,000 rows in `bench-q-0`, single-element array):

```
Limit (actual rows=50)         Buffers: shared hit=195   Execution Time: 13.942ms
  LockRows (actual rows=50)
    Sort (actual rows=50)      Sort Key: priority DESC, scheduled_at
      Index Scan using idx_harvest_tq_poll (actual rows=10000)
      Index Cond: queue_name = ANY('{bench-q-0}'::text[]) ...
```

**A `Sort` node is still present**, and the `Index Scan` still reads all
10,000 matching rows rather than the `LIMIT`ed 50 — structurally identical
to the 4-queue control, just cheaper in absolute buffers because the
underlying index range scanned is smaller. Cardinality dropped from 4 to
1; the plan shape did not change. (Execution time is noisier than buffers
at this scale — 13.942ms here vs. 4.391ms for the nominally more-expensive
control — and is not read as a signal on its own; buffer counts and plan
shape are.)

**Secondary arm** (single-queue scalar equality, same seed):

```
Limit (actual rows=50)         Buffers: shared hit=53    Execution Time: 0.111ms
  LockRows (actual rows=50)
    Index Scan using idx_harvest_tq_poll (actual rows=50)
    Index Cond: queue_name = 'bench-q-0' ...
```

No `Sort` node, bounded scan (50 rows read, matching `LIMIT`) — this is
the result the original pass reported and mistook for the answer to the
production-relevant question.

`grep -c` for a `Sort` node's constituent lines (`Sort` node line +
`Sort Key` line): control 2, single-queue `ANY` 2, single-queue scalar 0.
Full output archived at
[`results/multi_queue_control.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/multi_queue_control.explain.txt),
[`results/single_queue_any.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/single_queue_any.explain.txt),
[`results/single_queue_scalar.explain.txt`](apparatus/0006-claim-1177-baseline-queue-count/results/single_queue_scalar.explain.txt).

**Against the line:** the production-representative single-queue run
(`ANY` over a single-element array) still shows a `Sort` node — this
**refutes** the pre-registered hypothesis, cleanly, with no ambiguity to
adjudicate.

## 🏁 Verdict

**Refuted.** Multi-queue cardinality does not explain ledger #5's
`Sort`-node discrepancy against issue #1177's reported baseline: dropping
from 4 queues to 1, while keeping the `ANY()` operator shape the
production claim path actually uses, changes nothing structural about the
plan — `Sort` node present, full 10,000-row scan, both times. Only
switching the operator itself, from `ANY()` to scalar `=` — a change no
production code path makes — eliminates the `Sort` node and restores
bounded `LIMIT` pushdown (53 buffers, 0 `Sort` nodes, vs. 195 buffers, a
`Sort` node present, at identical cardinality and identical seed).

**This is a more consequential finding than the confirmed verdict it
replaces, and a worse one for the decision it feeds.** It does not resolve
ledger #5's "unresolved discrepancy against #1177" — it deepens it:
`docs/performance.md` reports that issue #1177's own baseline, using the
identical `ANY($1)` binding, returned a clean `Index Scan` with no `Sort`
at *some* undocumented cardinality. This apparatus cannot reproduce that
clean shape via `ANY()` at cardinality 1, the cardinality most favorable
to sort-elision working (a single-element array is, semantically, exactly
one value). Two possibilities remain open and this assay cannot
distinguish between them: (a) something about this apparatus or Postgres
instance differs from #1177's own reproduction in a way neither this nor
ledger #5's apparatus has isolated, or (b) `ANY()` genuinely never gets
sort-elision from this planner regardless of array cardinality, and
#1177's own reported clean baseline came from a scalar-equality query, not
an `ANY()` one, despite its own text describing the binding as `ANY($1)`.
Both are new, un-chartered pits, not settled by this apparatus.

**For the named decider:** the corrected, load-bearing finding is that
queue count is **not** a safe independent axis to treat as "cheap at low
cardinality" — a single-queue worker, using the query shape
`queue.rs` actually emits, gets the same `Sort`-and-full-scan plan a
4-queue worker does, at this apparatus's measured 10,000-row depth. Any
future #1340 re-charter should not assume single-queue deployments get a
free pass on this specific `ORDER BY`/`LIMIT` cost; that assumption, which
this assay's own first pass asserted, is now retracted. Whether the
`ANY()`-vs-`=` operator gap itself is worth a further re-charter (e.g.
checking whether Postgres's planner ever elides sort for `ANY()` over one
element, independent of this schema) is a new, separate, un-chartered
question — not answered here.

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
grep -c "Sort Key" results/single_queue_any.explain.txt      # 1
grep -c "Sort Key" results/single_queue_scalar.explain.txt   # 0
```

`schema.sql`, `single_queue_any_diagnostic.sql`,
`single_queue_diagnostic.sql`, `multi_queue_control.sql`, `run_assay.sh`,
and the full `results/*.txt` / `results/run.log` this report draws from
are archived alongside this report. No migration was added to
`autumn-harvest/migrations/`; no crate code changed. The prototype does
not merge.
