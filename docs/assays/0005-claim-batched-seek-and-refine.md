# ⛏️ Prospect: does a batched seek-and-refine rewrite fix issue #1340? (kill: 2 vs 1 batch on L4's own line, ledger #5)

> Status: **measured, then corrected post-review.** The pre-registration
> (`docs/rnd/2026-09-06-claim-batched-seek-and-refine-preregistration.md`,
> commit `ee3bc19`) was committed before the apparatus was built or run;
> nothing in it has been edited since. This report's numbers are from the
> apparatus's second run, after fixing a correctness bug and a
> mischaracterized mechanism that Codex's automated review of the PR caught
> in the first run (see "Post-review corrections" below) — the
> pre-registered lines themselves are unchanged.

## 🎯 Question

PR #1341 (issue #1177's doc fix) names, and explicitly declines to attempt,
the real fix for "any residual predicate defeats sort-elision": *"a
seek-and-refine restructuring of `claim_task_query()` — is architectural...
tracked separately as issue #1340."* Ledgers #3 and #4 each tested one
concurrency-gate-specific fix and killed both — #3 on a global index paying
per-row cost regardless of contention, #4 on unbounded retry count under
adversarial priority/saturation correlation. Neither is what "seek and
refine" names in the tracking issue.

**This assay's question:** does a **batched** seek-and-refine shape — one
query fetches the top `B=50` ordered candidates, a concurrency-recheck CTE
scoped only to that batch's own distinct keys (not the backlog's global key
cardinality), first eligible row wins in the same round trip, a second batch
only on total exhaustion — clear idle parity, the cardinality blowup
(cardinality-independently this time), the 256-key case, ledger #4's own
adversarial fixture in one round trip, and a 4x-deeper fixture in
proportionally more round trips?

**Decision this feeds / decider:** unchanged from #3/#4 — whoever owns the
queue/claim-path performance work; a pursue would be the trigger to open the
real architectural design conversation issue #1340 was deferred pending.

## ⚖️ Pre-registration

Committed in full at
[`docs/rnd/2026-09-06-claim-batched-seek-and-refine-preregistration.md`](../rnd/2026-09-06-claim-batched-seek-and-refine-preregistration.md)
(`ee3bc19`). Five lines, all five required for **pursue**, any one miss is
**kill**:

- **L1 (idle):** ≤300 buffers.
- **L2 (5,000-key hot-contention):** ≤160ms.
- **L3 (256-key hot-contention):** ≤2x same-run control.
- **L4 (ledger #4's 50-row adversarial fixture):** exactly 1 batch AND
  ≤100ms.
- **L5 (200-row adversarial fixture, 4x depth):** exactly 4 batches AND
  ≤400ms.

## 🔍 Prior art

Ledgers #3 and #4 (see Question above); PR #1341 / issue #1340 (the shape
this assay attempts); no existing prototype in the tree. Full detail in the
pre-registration's own Prior Art section — not repeated here.

## 🧪 Apparatus

`docs/assays/apparatus/0005-claim-batched-seek-and-refine/` (archived):
`schema.sql`/`seed.sql` copied unmodified from ledger #3/#4 (same indexes,
same non-adversarial fixture generator). New for this assay:

- `seed_adversarial_50.sql` — ledger #4's exact L4 fixture, copied
  byte-for-byte.
- `seed_adversarial_200.sql` — the same construction at 4x poison depth,
  for L5.
- `control.sql`/`control_raw.sql` — ledger #3/#4's committed-fix query,
  reused unmodified.
- `batch_claim.sql` — the candidate's single-batch query in isolation, for
  `EXPLAIN (ANALYZE, BUFFERS)` at the three non-adversarial scenarios.
- `claim_batched.sql` — a `plpgsql` function implementing the full
  multi-batch shape: each batch is one query (`candidates` CTE forced
  `MATERIALIZED` so it is computed once and reused for both the winner pick
  and the next batch's keyset cursor, not rescanned per reference), a
  second batch fetched only when the first returns no eligible row.
- `forced_index_diagnostic.sql` — added post-review; see below.
- `results/run.log`, `results/*.txt`, `results/*.explain.txt` — every run
  this report's numbers are drawn from.

**Stubs list (as declared in the pre-registration, unchanged):**

- Same orthogonal cuts as #3/#4 (`worker_info`, pause/build/rate-limit/
  capability predicates, the sticky-routing `CASE` key) — scoped to the
  concurrency-gate predicate only, not every residual predicate #1340
  nominally covers.
- **Not measured: concurrent-claimer lock contention.** This is a
  single-session apparatus, same as #3/#4. Batching locks up to `B=50` rows
  per attempt via `FOR UPDATE SKIP LOCKED` instead of 1 — whether that
  measurably hurts throughput under real concurrent claimers is exactly the
  "changes claim-fairness/latency guarantees under contention" risk PR #1341
  flagged as needing sign-off, and remains untested by this assay or any
  assay so far.
- `B=50` fixed, not swept.
- `claim_batched`'s `max_batches` bailout (50, arbitrary) is a stub; neither
  L4 nor L5 reaches it.

## 🔧 Post-review corrections (Codex)

Codex's automated review of the PR opened for this assay caught four issues
in the first run. All four were verified against this apparatus's own
`EXPLAIN` output before being accepted; two required a code fix and a
re-run, two were report/tooling corrections. **The pre-registered lines
were not touched by any of this** — verifying and fixing what the
apparatus actually measures is exactly the "regrade the assay conclusions"
half of the reviewer's own proposed remedy, not a re-registration.

1. **Correctness bug (P1): the keyset cursor was not unique.** `priority`
   and `scheduled_at` alone are not a unique key — `seed.sql` and
   `seed_adversarial_50.sql` assign one statement-stable `NOW()` value to
   many rows, so ties are the common case in this fixture, not an edge
   case. The original cursor predicate (`priority = cur AND scheduled_at >
   cur`) uses a strict `>`, so any unvisited row tied with the batch's own
   last row would be silently excluded from every later batch — not just
   skipped once, permanently. This apparatus's own L4 run only avoided
   tripping it by construction, not by correctness: all 50 poisoned rows in
   `seed_adversarial_50.sql` share one exact `(priority, scheduled_at)`
   tuple, and `batch_size` (50) happens to equal that tied group's size
   exactly, so the boundary lands cleanly after the whole group. A
   different `batch_size`, or the same `batch_size` against a differently
   sized tied group, would have silently skipped valid claimable rows.
   **Fixed:** `id` added as a third ORDER BY key and cursor column in both
   `claim_batched.sql` and `batch_claim.sql` (a plain three-column
   row-comparison still doesn't work across `priority DESC, scheduled_at
   ASC, id ASC`'s mixed directions, so the predicate stays an explicit OR
   chain). Re-run after the fix reproduces identical results in every
   scenario (same claimed row ids, same batch counts: L4 still 2, L5 still
   5) — confirming the bug was real but did not corrupt this run's specific
   numbers, only the apparatus's general correctness.

2. **Mechanism mischaracterization (P1): the candidate fetch is a Seq Scan
   of the whole backlog, not an index-ordered seek.** The report's original
   framing — "fetches the top `B` ordered candidates via
   `idx_harvest_tq_poll`" — is not what the archived `EXPLAIN` output shows.
   Every non-adversarial scenario's `candidates` CTE plans as `Seq Scan on
   harvest_task_queue (actual rows=10000 loops=1)` feeding a bounded top-N
   `Sort`, at this apparatus's 10,000-row/4-queue fixture depth — the same
   shape `control.sql` (the committed fix, reused unmodified) *also* plans
   as, confirmed directly in the archived `idle_256-control.explain.txt`.
   **This is not unique to this assay** — ledgers #3 and #4 inherited and
   reused the identical `schema.sql`/`seed.sql`, and their own `control.sql`
   plans the same way — but this report's prose claimed a mechanism
   (index-driven bounded seek) the evidence does not show, for either side
   of the comparison.

   Added `forced_index_diagnostic.sql` (`SET LOCAL enable_seqscan =
   off; enable_bitmapscan = off`, inside a rolled-back transaction —
   the same technique `docs/performance.md`'s own sticky-predicate
   diagnostic uses) to answer directly: is an index-ordered plan even
   reachable here, and what does it cost? **Forcing the index does not
   help.** `idle_256-forced_index.explain.txt` and
   `hot_5000-forced_index.explain.txt` both show `Index Scan using
   idx_harvest_tq_poll` — reachable — but with `actual rows=10000
   loops=1`, i.e. still every matching row, not a bounded probe, still
   feeding a `Sort` before `LIMIT 50` applies. Cost is *worse* than the
   natural Seq Scan plan (585 and 598 buffers respectively, vs. 181 and
   248): forcing the index changes *how* every row is read, not *how many*
   rows are read. `LIMIT` pushdown through the ordered scan does not happen
   for this query shape at this fixture depth, under either plan.

   **What this changes about the report's conclusions:** L1-L3's passing
   buffer/wall-clock numbers demonstrate that a 50-row batch costs about the
   same as a 1-row batch **at this specific 10,000-row backlog depth**, for
   both control and candidate alike — because at this depth, scanning and
   sorting the whole matching set is already cheap (~130-250 buffers) with
   or without a `LIMIT`. They do **not** demonstrate that batching keeps
   cost bounded *independent of backlog depth* — that would require varying
   backlog depth and showing the batch candidate's cost stays flat while
   something else grows, which this apparatus never tested (same gap ledger
   #3's and #4's own idle-case lines have, inheriting the same fixture).
   The recheck CTE's cardinality-independence finding (L2) is unaffected by
   this correction — that claim is about the *recheck*'s cost depending
   only on the batch's own key count, not the backlog's, and holds
   regardless of whether the candidate fetch itself is a Seq Scan or an
   Index Scan.

   **A further, unresolved discrepancy this diagnostic surfaced, explicitly
   out of this assay's scope:** `docs/performance.md`'s own issue #1177
   section states its `queue_name = ANY($1)`, no-residual-predicate baseline
   "shows `Index Scan using idx_harvest_tq_poll`, no `Sort` node at all,
   whatever `$1` held in that reproduction" — at a 255,020-row fixture. This
   apparatus's identically-shaped query (no residual predicate, `queue_name
   = ANY(4 values)`) does *not* reproduce that: it shows a `Seq Scan` (or,
   forced, an `Index Scan` that still reads every row) plus a `Sort`, at a
   10,000-row fixture. Table size, or the specific `$1` binding #1177 used,
   or something else, may explain the gap — not established here, and not
   resolved by this assay. Flagging as a new, separate, un-chartered
   question rather than investigating further inside this assay's box.

3. **Round-trip framing (P2): wall-clock timings measure one round trip
   total, not one per batch.** `claim_batched()`'s entire multi-batch loop
   runs server-side inside a single `plpgsql` function, invoked once via
   `SELECT * FROM claim_batched(...)`. L4's 2-batch resolution and L5's
   5-batch resolution each cost exactly one client/database round trip in
   this apparatus, not 2 or 5. The pre-registration's own framing ("each
   batch is one round trip") describes the *server-side query* cost per
   batch, which this apparatus does measure correctly — but the wall-clock
   numbers cannot be read as validating round-trip-bound scaling under real
   network latency between a caller and a remote database, which a
   production implementation issuing one batch per call would actually
   incur. (Ledger #4's `claim_deferred()` has the identical structure and
   the identical gap in its own wall-clock numbers — not corrected here,
   since #4 is closed and this note only applies going forward.) This does
   not change L4/L5's PASS on wall-clock, since the margins (35.7ms vs.
   ≤100ms, 72.8ms vs. ≤400ms) are wide enough that per-batch network latency
   at any realistic value would not plausibly erase them — but it does mean
   this assay does not independently establish that.

4. **Tooling (P2): `results/run.log` was hand-reconstructed, not
   regenerated by `run_assay.sh`.** `\timing`'s `Time: ... ms` lines are
   not redirected by `driver.sql`'s own `\o` commands (`\o` only redirects
   query *result* output), so the original `run.log` was manually
   transcribed from a captured terminal session after the first run — a
   second run producing different numbers would not have updated it.
   **Fixed:** `run_assay.sh` now pipes the whole session through `tee
   results/run.log`, and `driver.sql` gained `\echo` labels before each
   timed section, so `results/run.log` is regenerated, labeled, and current
   on every invocation.

## 📊 Assay

All measurements from `docs/assays/apparatus/0005-claim-batched-seek-and-refine/results/`
(`run.log`, `*.txt`, `*.explain.txt`), from the apparatus's second run (post
the corrections above), one continuous psql session.

**Buffers (L1, idle: 10,000 backlog, 4 queues, 256 keys, 0 RUNNING):**

| | buffers |
|:--|--:|
| control (committed fix) | 132 |
| candidate (`batch_claim.sql`, B=50) | 181 |
| candidate, index forced (`forced_index_diagnostic.sql`) | 585 |

**Wall-clock, raw `\timing` (no `EXPLAIN` instrumentation on either side, matching #4's methodology):**

| scenario | keys | running | control (`control_raw.sql`) | candidate (`claim_batched()`) | batches |
|:--|--:|--:|--:|--:|--:|
| idle_256 | 256 | 0 | 6.699 ms | 9.220 ms | 1 |
| hot_256 | 256 | 2,000 | 144.105 ms | 10.630 ms | 1 |
| hot_5000 | 5,000 | 2,000 | 1,058.862 ms | 6.336 ms | 1 |
| **l4_adversarial (50 poison)** | 256 | 20 | — (n/a) | **35.723 ms** | **2** |
| **l5_adversarial (200 poison)** | 256 | 20 | — (n/a) | **72.759 ms** | **5** |

(Wall-clock figures carry run-to-run noise on this box, same as every prior
ledger entry — e.g. `hot_256` control ranged 144-222ms across this assay's
two runs. Read them for order of magnitude relative to the same run's own
control, not as exactly reproducible absolutes.)

Equivalence check: candidate claimed the identical row id to control in
every non-adversarial scenario (`results/equivalence_idle_256.txt`:
`54293`/`54293`; `equivalence_hot_256.txt`: `64293`/`64293`; hot_5000's two
raw outputs both read `22001`). Correctness in both adversarial scenarios
was re-confirmed after the tiebreaker fix: same claimed row, same batch
counts, in both L4 and L5.

**Against the lines:**

- **L1 — PASS.** 181 buffers vs. ≤300 — 1.66x the committed fix's 132, well
  inside the line and two orders of magnitude below ledger #3's 10,130-buffer
  kill. (See "Post-review corrections" above for what this number does and
  does not establish about *why* it's cheap.)
- **L2 — PASS, decisively.** 6.336ms vs. ≤160ms — **25.2x** inside the line,
  **167.1x** faster than this run's own control (1,058.862ms). The
  recheck CTE's cost stays independent of global key cardinality (5,000
  here vs. 256 at L1/L3) because it only ever touches the ≤50 distinct keys
  actually present in the fetched batch — the one causal claim this
  correction round left intact, since it concerns the recheck, not the
  candidate fetch.
- **L3 — PASS, decisively.** 10.630ms vs. ≤288.21ms (2x control's
  144.105ms) — **13.6x faster than control outright**, not just inside the
  line.
- **L4 — FAIL on the batch-count sub-criterion.** Wall-clock passes cleanly
  (35.723ms vs. ≤100ms, **2.8x** inside the line) — but the shape resolved
  in **2 batches, not the registered 1**. This is a fencepost error in the
  pre-registration itself, not a mechanism finding: with `B=50` and exactly
  50 poisoned rows ranked ahead of the one claimable row, batch 1 fetches
  precisely the 50 poisoned rows (nothing left over for the claimable row,
  which is ranked 51st), so batch 2 is structurally required regardless of
  how the batch mechanism performs. The correct line should have been
  "`ceil((poison_depth + 1) / B)` batches," i.e. 2 for this fixture — the
  registered "1" undercounted by exactly the one slot the claimable row
  itself occupies.
- **L5 — FAIL on the same sub-criterion, same root cause.** Wall-clock
  passes cleanly (72.759ms vs. ≤400ms, **5.5x** inside the line) — but the
  shape resolved in **5 batches, not the registered 4**. Same fencepost:
  `ceil((200 + 1) / 50) = 5`, not `ceil(200/50) = 4`.

**Riskiest assumption, checked first:** the risk this shape's own mechanism
introduces (per the pre-registration) was whether cost degrades linearly in
*batch count* rather than catastrophically, the way ledger #4's shape
degraded catastrophically in *attempt count*. That holds: 35.723ms at 2
batches, 72.759ms at 5 batches — a 2.5x batch-count increase producing a
2.04x wall-clock increase, consistent with cost scaling as `batches ×
(cost of one batch)`. This is a real, substantive answer, but per the
post-review correction above it is a narrower one than originally framed:
it confirms batching degrades gracefully in *batch count* at this backlog
depth, not that either the candidate or the control avoids `O(backlog)`
scaling as depth grows — that remains untested.

## 🏁 Verdict

**Kill**, per the pre-registered rule: two of five lines (L4, L5) miss on
their exact-batch-count sub-criterion, and "any one miss is kill" applies
regardless of how the other three lines perform or why the miss happened.

**This kill is on the pre-registration's own arithmetic, not on the
candidate mechanism's batch-count scaling** — every wall-clock line clears
by 2.8x-167x, and the batch-count scaling itself is linear as designed. But
post-review correction narrows what this assay can claim even if the
batch-count lines had been written correctly: this apparatus never
established that batching bounds cost independent of backlog depth, only
that it doesn't cost meaningfully more than the (already `O(backlog)` at
this fixture depth) current committed fix, at one fixed depth. The one
claim that survives fully intact is the recheck CTE's cardinality
independence (L2) — a real, narrower, still-useful property, but not the
full "seek and refine" story issue #1340 was named for.

**What this assay establishes, and does not:**

- Establishes: a single-round-trip-per-batch shape with a batch-scoped
  recheck CTE is mechanically sound (correct row every time, including
  under adversarial saturation) and does not cost meaningfully more than
  the current fix, at this apparatus's one tested backlog depth.
- Establishes: batch-count scaling under adversarial depth is linear, not
  catastrophic — a real, positive finding about *this* mechanism's shape,
  independent of the batch-count-line fencepost bug.
- Does **not** establish: that the candidate fetch itself avoids `O(backlog)`
  scanning as backlog depth grows — untested, and the forced-index
  diagnostic suggests this specific query shape may not get `LIMIT`
  pushdown at any depth without further work (see the `docs/performance.md`
  #1177-baseline discrepancy noted above).
- Does **not** establish: real-network round-trip cost at 2-5 batches
  (measured as 1 round trip here).
- Does **not** establish: concurrent-claimer lock-contention cost (never
  measured by this or any prior concurrency-gate assay).

**Explicitly not this assay's finding, and an explicit re-charter, not an
edit:** a corrected pre-registration (`ceil((poison_depth+1)/B)` batches as
the line) would likely still pass L4/L5's wall-clock sub-criterion on this
apparatus, for the reasons above — but a re-charter aimed at the *real*
question issue #1340 asks (does this shape's cost stay bounded as backlog
depth grows, independent of the multi-queue `LIMIT`-pushdown question this
assay surfaced but did not resolve) would need a fixture that varies
backlog depth and a resolved account of why this apparatus's baseline
doesn't match `docs/performance.md`'s own #1177 baseline. Both are new,
un-chartered pits, not fixed by re-running this assay's own lines with
corrected arithmetic.

## 🔬 Reproduce

```
sudo -u postgres createdb prospect_assay5   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0005-claim-batched-seek-and-refine
PGDATABASE=prospect_assay5 ./run_assay.sh
cat results/run.log
grep "Buffers: shared hit=181" results/idle_256-batch_claim.explain.txt
grep "Index Scan using idx_harvest_tq_poll" results/idle_256-forced_index.explain.txt
```

`schema.sql`, `seed.sql`, `seed_adversarial_50.sql`, `seed_adversarial_200.sql`,
`control.sql`, `control_raw.sql`, `batch_claim.sql`, `claim_batched.sql`,
`forced_index_diagnostic.sql`, and `driver.sql` are archived alongside
`run_assay.sh` in this directory, along with the full `results/*.txt` /
`results/*.explain.txt` output and `results/run.log` (regenerated by
`run_assay.sh` itself, per the tooling fix above) this report's tables are
drawn from. No migration was added to `autumn-harvest/migrations/`; no
crate code changed. The prototype does not merge.
