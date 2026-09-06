# ⛏️ Prospect: does a batched seek-and-refine rewrite fix issue #1340? (kill: 2 vs 1 batch on L4's own line, ledger #5)

> Status: **measured.** The pre-registration
> (`docs/rnd/2026-09-06-claim-batched-seek-and-refine-preregistration.md`,
> commit `ee3bc19`) was committed before the apparatus was built or run;
> nothing in it has been edited since. This report is the Apparatus, Assay,
> Verdict and Reproduce sections appended afterward, with the actual numbers.

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
  and the next batch's keyset cursor, not rescanned per reference), keyset
  continuation via an explicit `priority < cur OR (priority = cur AND
  scheduled_at > cur)` predicate (a plain `<` row comparison does not work
  across columns with opposite sort directions), a second batch fetched only
  when the first returns no eligible row.
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

## 📊 Assay

All measurements from `docs/assays/apparatus/0005-claim-batched-seek-and-refine/results/`
(`run.log`, `*.txt`, `*.explain.txt`), one continuous psql session.

**Buffers (L1, idle: 10,000 backlog, 4 queues, 256 keys, 0 RUNNING):**

| | buffers |
|:--|--:|
| control (committed fix) | 132 |
| candidate (`batch_claim.sql`, B=50) | 181 |

**Wall-clock, raw `\timing` (no `EXPLAIN` instrumentation on either side, matching #4's methodology):**

| scenario | keys | running | control (`control_raw.sql`) | candidate (`claim_batched()`) | batches |
|:--|--:|--:|--:|--:|--:|
| idle_256 | 256 | 0 | 10.228 ms | 8.338 ms | 1 |
| hot_256 | 256 | 2,000 | 221.588 ms | 8.592 ms | 1 |
| hot_5000 | 5,000 | 2,000 | 1,465.765 ms | 8.158 ms | 1 |
| **l4_adversarial (50 poison)** | 256 | 20 | — (n/a) | **14.728 ms** | **2** |
| **l5_adversarial (200 poison)** | 256 | 20 | — (n/a) | **33.944 ms** | **5** |

Equivalence check: candidate claimed the identical row id to control in
every non-adversarial scenario (`results/equivalence_idle_256.txt`,
`equivalence_hot_256.txt`, and hot_5000's two raw outputs both read
`22001`). A direct, independent re-check of L4 (`state`/`concurrency_key`
of the claimed row after the call, in a rolled-back transaction) confirms
the claimed row is always the unpoisoned one (`concurrency_key IS NULL`)
and never a saturated-key row — correctness held in every scenario run.

**Against the lines:**

- **L1 — PASS.** 181 buffers vs. ≤300 — 1.66x the committed fix's 132, well
  inside the line and two orders of magnitude below ledger #3's 10,130-buffer
  kill. The batch scan (`LIMIT 50` instead of `LIMIT 1`) costs a little more
  than the single-row case but stays a small, bounded scan — not a
  backlog-wide one.
- **L2 — PASS, decisively.** 8.158ms vs. ≤160ms — **19.6x** inside the line,
  **179.7x** faster than this run's own control (1,465.765ms). Confirms the
  batch-scoped recheck CTE's cost does not depend on the backlog's global
  key cardinality (5,000 here vs. 256 at L1/L3) — it only ever touches the
  ≤50 distinct keys actually present in the fetched batch, which is exactly
  the mechanism ledger #3's global partial index lacked.
- **L3 — PASS, decisively.** 8.592ms vs. ≤443.176ms (2x control's
  221.588ms) — **25.8x faster than control outright**, not just inside the
  line.
- **L4 — FAIL on the batch-count sub-criterion.** Wall-clock passes cleanly
  (14.728ms vs. ≤100ms, **6.8x** inside the line) — but the shape resolved
  in **2 batches, not the registered 1**. This is a fencepost error in the
  pre-registration itself, not a mechanism finding: with `B=50` and exactly
  50 poisoned rows ranked ahead of the one claimable row, batch 1 fetches
  precisely the 50 poisoned rows (nothing left over for the claimable row,
  which is ranked 51st), so batch 2 is structurally required regardless of
  how the batch mechanism performs. The correct line should have been
  "`ceil((poison_depth + 1) / B)` batches," i.e. 2 for this fixture — the
  registered "1" undercounted by exactly the one slot the claimable row
  itself occupies. Per the pre-registered rule, a numeric miss is a kill
  regardless of why it was set wrong; it is not this report's place to
  retroactively read the line as "2" because that's what would pass.
- **L5 — FAIL on the same sub-criterion, same root cause.** Wall-clock
  passes cleanly (33.944ms vs. ≤400ms, **11.8x** inside the line) — but the
  shape resolved in **5 batches, not the registered 4**. Same fencepost:
  `ceil((200 + 1) / 50) = 5`, not `ceil(200/50) = 4`. The registered
  "4" made the same off-by-one mistake L4's line did, for the identical
  reason.

**Riskiest assumption, checked first:** the risk this shape's own mechanism
introduces (per the pre-registration) was whether cost degrades linearly in
*batch count* rather than catastrophically, the way ledger #4's shape
degraded catastrophically in *attempt count*. That holds: 14.728ms at 2
batches, 33.944ms at 5 batches — a 2.5x batch-count increase producing a
2.30x wall-clock increase, consistent with cost scaling as
`batches × (cost of one batch)` and not as `poison_depth × (cost of one
full backlog scan)` (which is what killed #4: a 4x poison-depth increase
there would have meant roughly a 4x *attempt* increase at *full-scan* cost
each, not a fixed per-attempt cost). This is the substantive question the
assay set out to answer, and the data answers it: **yes, the batching
mechanism itself behaves exactly as designed** — the kill below is not a
finding against that mechanism.

## 🏁 Verdict

**Kill**, per the pre-registered rule: two of five lines (L4, L5) miss on
their exact-batch-count sub-criterion, and "any one miss is kill" applies
regardless of how the other three lines perform or why the miss happened.

**This kill is on the pre-registration's own arithmetic, not on the
candidate mechanism.** Every other signal in this assay is a clean pass,
several by wide margins: idle cost stays in the same order of magnitude as
the committed fix (L1), the cardinality-independence property the mechanism
was specifically designed to have is confirmed directly (L2 costs the same
order of magnitude as L3 despite a 20x difference in global key
cardinality), and wall-clock at both adversarial depths clears its line by
6.8x and 11.8x respectively — the *only* thing that failed is a batch-count
integer the pre-registration itself miscalculated by exactly one, in the
same direction, on both adversarial lines. Per this role's own rule against
moving the kill line after the fact, that arithmetic error is reported as a
kill, not quietly reread as a pass — but it is reported precisely, so it
does not read as evidence against a mechanism the same data otherwise
supports.

**What this assay establishes, and does not:** it establishes that a
single-round-trip-per-batch, batch-scoped-recheck shape is mechanically
sound and cheap for the concurrency-gate predicate specifically, at every
scenario this apparatus can construct, including the exact adversarial
fixture that killed ledger #4. It does **not** establish a pursue verdict,
because the pre-registered acceptance test — worded with the fencepost bug
— was not met by its own letter. It also does not touch the
concurrent-claimer lock-contention question (stubs list): batching locks up
to `B` rows per attempt via `FOR UPDATE SKIP LOCKED`, and nothing in this
single-session apparatus can say whether that measurably costs throughput
under real concurrency — the same open risk PR #1341 named and this role
still has not measured, across three assays now.

**Explicitly not this assay's finding, and an explicit re-charter, not an
edit:** a corrected pre-registration (`ceil((poison_depth+1)/B)` batches as
the line, same wall-clock lines) run against the identical apparatus already
archived here would very likely pass L4 and L5 outright — the wall-clock
margins (6.8x, 11.8x) are wide enough that the correction alone would not
plausibly flip the wall-clock sub-criterion, only the batch-count one this
report is declining to silently re-grade. That is a prediction, not a
result; per this role's charter it is stated as one and would need its own
committed pre-registration to count as a verdict. The concurrent-lock-
contention risk above is a second, independent open question a re-charter
would still need to either fold in or explicitly continue deferring.

## 🔬 Reproduce

```
sudo -u postgres createdb prospect_assay5   # or any local, non-production Postgres 16
cd docs/assays/apparatus/0005-claim-batched-seek-and-refine
PGDATABASE=prospect_assay5 ./run_assay.sh
cat results/run.log
grep "Buffers: shared hit=181" results/idle_256-batch_claim.explain.txt
```

`schema.sql`, `seed.sql`, `seed_adversarial_50.sql`, `seed_adversarial_200.sql`,
`control.sql`, `control_raw.sql`, `batch_claim.sql`, `claim_batched.sql`, and
`driver.sql` are archived alongside `run_assay.sh` in this directory, along
with the full `results/*.txt` / `results/*.explain.txt` output and
`results/run.log` this report's tables are drawn from. No migration was
added to `autumn-harvest/migrations/`; no crate code changed. The prototype
does not merge.
