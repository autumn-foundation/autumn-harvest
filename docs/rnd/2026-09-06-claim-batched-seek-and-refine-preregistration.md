# ⛏️ Prospect pre-registration: does a batched seek-and-refine rewrite fix issue #1340? (assay ledger #5)

**Committed:** 2026-09-06T09:09:44Z, before any apparatus was built or measurement taken.
This document is the contract; the report that follows it is graded against
these lines, not against whatever the numbers turn out to be.

## 🎯 Question

`docs/changelog.d/pr-1177-claim-residual-predicate-sort-elision.md` closes
issue #1177 (any residual `WHERE` predicate on `claim_task_query()` defeats
`idx_harvest_tq_poll`'s sort-elision, forcing a full-backlog scan+sort per
claim) by naming, and explicitly declining to attempt, the real fix: *"a
seek-and-refine restructuring of `claim_task_query()` — is architectural,
changes claim-fairness/latency guarantees under contention... tracked
separately as issue #1340."* Nothing in `docs/rnd/`, `docs/assays/`, or the
ledger (`docs/assays/README.md`, #1-#4) names or tests issue #1340. It is an
open pit.

Two prior assays each tried one instance of a fix, both for the
concurrency-key gate specifically (not the general residual-predicate
problem #1340 is scoped to), and both killed:

- Ledger #3: a global partial index so the gate's recheck touches
  `harvest_task_queue` directly. Killed on idle cost — the index rewrite
  loses `ORDER BY … LIMIT` pushdown entirely, so every claim pays a real
  per-row index probe across the whole backlog regardless of contention.
- Ledger #4: deferring the gate to a per-attempt authoritative recheck,
  retrying the *entire* candidate-selection query on failure. Killed on
  adversarial retry depth — cost is structurally `attempts × (cost of one
  full claim attempt)`, and nothing bounds attempt count when high-priority
  rows cluster on saturated keys.

**Falsifiable question:** does a **batched** seek-and-refine shape — one
query fetches the top `B` ordered `PENDING` candidates via
`idx_harvest_tq_poll` (`FOR UPDATE SKIP LOCKED`, no residual predicate in
the scan itself), a small `MATERIALIZED` CTE computes the concurrency-gate
recheck scoped only to the ≤`B` distinct concurrency keys actually present
*in that batch* (not the backlog's global key cardinality — the mechanism
that killed #3), and the first eligible row in priority order is picked in
the same round trip — simultaneously:

1. keep idle cost in the same order of magnitude as the current committed fix,
2. fix the 5,000-key hot-contention blowup (1,600ms) **without depending on
   global key cardinality** (unlike #3's index, whose cost was driven by
   backlog depth, not key count, but which paid for every row regardless),
3. not regress the 256-key hot-contention case,
4. resolve ledger #4's 50-row adversarial fixture in a single batch (one
   round trip, not 51), and
5. degrade **linearly in batch count**, not catastrophically, when
   adversarial depth exceeds one batch.

## 👤 Decision this feeds

Whether a batched seek-and-refine shape is worth the real architectural
design-and-sign-off effort issue #1340 was deferred pending (the atomicity,
fairness-under-contention, and `FOR UPDATE`-locking-footprint questions
`docs/rnd/2026-09-03-redis-queue-worker-integration-deferral.md` and PR
#1341 both flag but do not resolve). A **pursue** verdict here is a reason
to open that design conversation with real numbers; a **kill** closes this
specific shape the same way #3 and #4 closed theirs, narrowing what "seek
and refine" could still mean.

**Decider:** same as ledger #3/#4 — whoever owns the queue/claim-path
performance work (`docs/performance.md`'s own maintainer thread). This
assay does not decide; it produces the numbers issue #1340 was deferred
without.

## ⚖️ Success / kill criteria (numeric, set now)

Same apparatus family as ledger #3/#4 (identical `schema.sql`/`seed.sql`,
same 4-queue/10,000-row backlog, same `NON_BLOCKING_CAP` and
hot-contention shapes) so results are directly comparable. Batch size
`B = 50` is fixed for every line below, chosen to exactly match ledger #4's
own L4 adversarial-fixture depth (so L4 below tests "one batch is enough"
at the identical fixture ledger #4 failed on, not an easier one picked after
the fact).

Five lines. **All five must clear for pursue; any one miss is kill** — same
rule as #3 and #4.

- **L1 — idle must stay in the current fix's order of magnitude.** 10,000-row
  backlog, 4 queues, 256 keys, 0 RUNNING: candidate's total buffers (batch
  select + CTEs) **≤ 300** (control/committed fix measures ~132 on this
  apparatus; 300 is generous headroom over a 50-row batch scan while staying
  two orders of magnitude below #3's 10,130-buffer kill).
- **L2 — 5,000-key hot-contention must clear the same 10x margin #3 and #4
  used.** 5,000 distinct keys, 2,000 RUNNING rows: candidate wall-clock
  **≤ 160ms** (10x the documented 1,600ms). Distinguishing evidence this line
  is meant to surface: the candidate's recheck CTE must cost the same
  regardless of whether this line is run at 256 or 5,000 keys, since it only
  ever touches the batch's own ≤50 distinct keys — checked directly by
  comparing L2's and L3's CTE cost, not just each against its own line.
- **L3 — 256-key hot-contention must not regress.** Same 2,000-RUNNING-row
  shape at 256 keys: candidate wall-clock **≤ 2x** whatever the control
  measures on this apparatus in the same run (same rule as #4's L3).
- **L4 — ledger #4's own adversarial fixture, resolved in one batch.** The
  identical fixture (50 highest-priority `PENDING` rows round-robined across
  10 saturated concurrency keys, one claimable row immediately behind them):
  candidate must (a) resolve in **exactly 1** batch fetch (one round trip)
  and (b) total wall-clock **≤ 100ms** — the same line #4 missed at 313.8ms.
- **L5 — deeper adversarial depth must degrade linearly in batch count, not
  catastrophically.** A fixture with **200** poisoned `PENDING` rows (4x
  ledger #4's depth, same 10 saturated keys, round-robined) ahead of the one
  claimable row, with `B` still 50: candidate must (a) resolve in **exactly
  4** batches (`ceil(200/50)`) and (b) total wall-clock **≤ 400ms** (4x L4's
  100ms line — the number this shape's own mechanism predicts if cost is
  `batches × per-batch-cost`, not attempts × per-row-cost as in #4).

**Correctness, checked alongside every line above (not a numbered line, a
precondition for any of them counting):** in every non-adversarial scenario
the candidate must claim the identical row id the control claims. In both
adversarial scenarios (L4, L5) the batch mechanism must not claim any of the
poisoned rows (a saturated key must never be claimed over its cap) and must
not claim fewer or more batches than the count stated in that line.

## 🔍 Prior art

- `docs/changelog.d/pr-1177-claim-residual-predicate-sort-elision.md` and
  `docs/performance.md`'s "Any residual predicate defeats sort-elision"
  section — names issue #1340 and the seek-and-refine idea; does not
  attempt it. This assay is that attempt, for the concurrency-gate instance
  of the general problem (the same scoping ledger #3/#4 used — the general
  #1340 problem spans every residual predicate on the claim path, and one
  assay cannot cheaply cover all of them; picking the same predicate #3/#4
  already instrumented keeps this comparable to existing lines instead of
  opening a second, uncontrolled variable).
- `docs/assays/0003-concurrency-gate-cardinality-index.md` (ledger #3) —
  established that a global index rewrite pays per-row cost regardless of
  contention; this assay's CTE is deliberately scoped to the batch, not the
  backlog, specifically to avoid re-triggering that failure mode.
- `docs/assays/0004-concurrency-gate-deferred-recheck.md` (ledger #4) —
  established the retry-count mechanism and its adversarial fixture; L4/L5
  above reuse its exact fixture shape and line (L4) or a named multiple of
  it (L5) rather than inventing a new adversarial construction.
- `docs/rnd/2026-09-03-redis-queue-worker-integration-deferral.md` — the
  record that first named the sort-key/residual-predicate defect as "the
  single biggest lever" and left it unscoped; not a re-dig, this is the
  follow-up it points at.
- No existing `plpgsql` or SQL prototype anywhere in the tree implements
  batch candidate fetch + batch-scoped recheck for this query. Not a re-dig.

## 🧪 Apparatus (plan — built after this commit)

`docs/assays/apparatus/0005-claim-batched-seek-and-refine/`, reusing
ledger #3/#4's `schema.sql` and `seed.sql` unmodified (same indexes, same
non-adversarial fixture generator, for direct comparability), plus:

- `seed_adversarial_50.sql` — ledger #4's exact L4 fixture, copied byte-for-
  byte for reuse.
- `seed_adversarial_200.sql` — the same construction at 4x depth for L5.
- `control.sql` / `control_raw.sql` — ledger #3/#4's committed-fix query,
  reused unmodified as the control.
- `batch_claim.sql` — the candidate's single-round-trip batch query in
  isolation, for `EXPLAIN (ANALYZE, BUFFERS)` at the non-adversarial
  scenarios.
- `claim_batched.sql` — a `plpgsql` function implementing the multi-batch
  continuation for L4/L5 (loops only when a whole batch is exhausted with no
  eligible row, using `(priority, scheduled_at)` keyset continuation between
  batches — not a per-row exclusion list).

**Stubs list (declared now, before apparatus, since some are already known
from the question's own shape):**

- Same orthogonal cuts as #3/#4: `worker_info`, `paused_queues`,
  `paused_activities`, build-routing, workflow-pause, rate-limit,
  capability-label clauses, and the sticky-routing `CASE` key — this assay
  is scoped to the concurrency-gate predicate exactly as #3/#4 were, not to
  every residual predicate #1340 nominally covers.
- **Not measured: concurrent-claimer lock contention.** Batching locks up to
  `B` rows per attempt via `FOR UPDATE SKIP LOCKED` instead of 1; this
  assay is a single psql session, same as #3/#4, and cannot measure whether
  `B` concurrent-claimer-visible locks per attempt reduces throughput under
  real concurrency (`docs/assays/0002-...md`'s multi-claimer harness could,
  but building it is out of this box). This is exactly the "changes
  claim-fairness/latency guarantees under contention" risk PR #1341 flagged
  as needing sign-off — this assay narrows the *shape* of a candidate fix
  and its single-session cost, not that specific contention risk. Any
  pursue verdict here is conditional on that risk being measured separately
  before a real build.
- `B = 50` is fixed, not swept. Whether 50 is the right production batch
  size (bigger batches amortize round trips further but lock more rows;
  smaller batches lock fewer rows but need more round trips on deep
  adversarial runs) is exactly the "then what" tuning question a real design
  would need — not resolved here.
- `claim_batched`'s batch-count bailout (a `max_batches` cap, arbitrary) is
  itself a stub, same shape as #4's `max_attempts`: this assay's L5 doesn't
  reach it, so it does not need a principled answer for what happens on
  exhaustion.

## 📊 Assay — to be filled in after apparatus runs

## 🏁 Verdict — to be filled in after apparatus runs

## 🔬 Reproduce — to be filled in after apparatus runs
