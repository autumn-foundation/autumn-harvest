# ⛏️ Prospect: does a batched seek-and-refine rewrite fix issue #1340? (kill: 2 vs 1 batch on L4's own line, ledger #5)

> Status: **measured, then corrected across five rounds of post-review.**
> The pre-registration
> (`docs/rnd/2026-09-06-claim-batched-seek-and-refine-preregistration.md`,
> commit `ee3bc19`) was committed before the apparatus was built or run;
> nothing in it has been edited since. This report's numbers are from the
> apparatus's sixth run (the second of two back-to-back invocations that
> together verified item 9's idempotency fix), after Codex's automated
> review of the PR caught (in order): a mischaracterized fetch mechanism, a
> keyset-cursor correctness bug, a missing concurrency-safety recheck, a
> stale cost citation, a non-idempotent apparatus schema, and two places
> where the report's own prose fell out of sync with those fixes (see
> "Post-review corrections" below for all twelve items) — the
> pre-registered lines themselves are unchanged throughout.

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
  multi-batch shape: each batch is one `FOR cand IN SELECT ...` fetch (`FOR
  UPDATE SKIP LOCKED`, keyset-cursor `WHERE`), walked procedurally in
  priority order — for each candidate with a concurrency key,
  `pg_try_advisory_xact_lock` plus a fresh `COUNT` against
  `harvest_task_queue` directly (the production path's own mechanism, not
  a batch-wide snapshot CTE — see "Post-review corrections," item 5, which
  replaced an earlier, unsafe CTE-based version this bullet described
  before that fix). A second batch is fetched only when the first is
  exhausted with no eligible row.
- `forced_index_diagnostic.sql` — added post-review; see below.
- `forced_index_no_tiebreak_diagnostic.sql` — added post-review (round 2);
  see below.
- `recheck_cost_diagnostic.sql` — added post-review (round 3); see below.
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
   not change L4/L5's PASS on wall-clock, since the margins (about 35ms vs.
   ≤100ms, about 72ms vs. ≤400ms) are wide enough that per-batch network latency
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

**Round 2 (same PR, next review pass on the corrected commit):**

5. **Correctness bug (P1): the winner check used a stale batch-wide
   snapshot, not the production path's per-candidate authoritative
   recheck.** The corrected `claim_batched.sql` above still picked its
   winner from `running_counts`, a `MATERIALIZED` CTE computed once per
   batch from a pre-claim read — the same shape ledger #3's control query
   uses, but without the production path's `pg_try_advisory_xact_lock`
   serialization (`autumn-harvest/src/queue.rs:750-770`) or ledger #4's own
   `claim_deferred()`, which has that lock. Two concurrent callers landing
   on *different* candidates sharing a concurrency key could each read the
   same stale count and both commit, exceeding the cap — the exact race the
   advisory lock exists to prevent. This is a correctness gap, not (only)
   the "unmeasured lock-contention performance" the pre-registration's
   stubs list scoped it as. **Fixed:** `claim_batched.sql` now fetches a
   batch via one query (unchanged cost), then walks the already-fetched,
   already-locked rows procedurally — for each candidate with a
   concurrency key, `pg_try_advisory_xact_lock` plus a fresh `COUNT`
   against `harvest_task_queue` directly (the identical mechanism, and the
   identical isolated query shape ledger #4 already measured — see item 8
   below for the corrected cost, which is *not* uniformly ~1 buffer),
   moving to the next already-fetched candidate on a failed lock or a
   failed count — no new backlog-wide query either way. Re-run after the
   fix reproduces identical claimed rows and batch counts (L4 still 2, L5
   still 5) — the snapshot-vs-authoritative distinction is invisible in a
   single-session apparatus by construction (nothing else is racing),
   which is exactly why it took a second review pass, not this apparatus's
   own testing, to catch. `batch_claim.sql`'s isolated `EXPLAIN` no longer
   models the recheck at all (it was modeling the now-removed, incorrect
   snapshot mechanism) — it measures only the fetch step; the recheck's
   per-candidate cost is addressed separately in item 8, and its
   contribution to end-to-end cost is visible directly in this section's
   `claim_batched()` wall-clock figures below.

6. **Rebuttal (P2, not accepted): the `id` tiebreak was proposed as the
   actual explanation for the `Sort` node, not backlog depth or the
   multi-queue binding.** A reviewer suggested that `finding 2`'s "Index
   Scan, still `actual rows=10000`" result was an artifact of *this
   assay's own* `id ASC` tiebreak — absent from `idx_harvest_tq_poll` and
   from `docs/performance.md`'s #1177 baseline's `ORDER BY` — forcing a
   sort of the (large, tied) backlog by `id`, and that flagging an
   unresolved #1177-baseline discrepancy was therefore a false lead.
   **Checked directly, not accepted:** `forced_index_no_tiebreak_diagnostic.sql`
   re-runs the identical forced-index query with the `id` tiebreak removed
   — byte-for-byte #1177's own `ORDER BY priority DESC, scheduled_at ASC`,
   same `enable_seqscan`/`enable_bitmapscan` bias. The `Sort` node and
   `actual rows=10000` both persist, essentially unchanged (582 buffers
   vs. 585 with the tiebreak) — directly refuting the proposed mechanism.
   The discrepancy against `docs/performance.md`'s own #1177 baseline
   therefore stands as unresolved by this assay, as originally reported;
   this round only adds a direct test ruling out one specific candidate
   explanation for it, which a future depth-varying or binding-varying
   re-charter would otherwise have had to rule out itself.

**Round 3 (same PR, third review pass, on the round-2 correctness fix):**

7. **Process objection (P1, addressed, not re-chartered): does grading the
   round-2 correctness fix against the original pre-registered lines break
   this role's own pre-registration discipline?** The pre-registration's
   prose specifically describes the candidate's recheck as "a small
   `MATERIALIZED` CTE" computed once per batch — item 5's fix replaced that
   with a per-candidate procedural `pg_try_advisory_xact_lock` + `COUNT`
   loop, a different implementation shape than the one named in the
   committed text, and a reviewer argued that re-grading the corrected
   numbers against the *original* L1-L5 lines, rather than treating this as
   a new, re-chartered experiment, risks exactly the goalpost-moving this
   role's charter bans.

   **This was not dismissed; here is why it wasn't treated as a
   re-charter.** The pre-registered *lines* (buffer counts, wall-clock
   thresholds, batch counts) never changed — only the candidate's
   *implementation* of the concurrency-gate check changed, and it changed
   because the original implementation was unsound (item 5), not because a
   sound implementation gave an inconvenient number. Testing a candidate
   that violates the production path's own safety invariant was never a
   valid instance of "batched seek-and-refine" to begin with; fixing that
   is closer to ledger #2's own precedent (`docs/assays/README.md`, #2:
   "Post-review (Codex) caught the first apparatus silently reading only
   one of the four queues, fixed by rotating queue order per call and
   directly verified") than to moving a kill line after seeing a result.
   That said, the concern about *cost* changing was not just asserted away:
   this apparatus's own L4/L5 adversarial fixtures already exercise
   multiple in-batch rejections under the corrected mechanism (2 and 5
   batches respectively, each batch walking up to 50 candidates
   procedurally), and their wall-clock numbers did not change materially
   across the pre-fix and post-fix runs (round 1: 35.7ms/72.8ms; round 2:
   34.9ms/71.7ms; round 3 re-run: 36.2ms/74.4ms — all within this box's own
   run-to-run noise band). So the specific risk named — that switching
   mechanisms could hide a cost regression behind an unchanged verdict —
   was checked, not just argued past.

   **What this does not cover, flagged honestly rather than claimed
   solved:** L4/L5's adversarial fixtures use a small `RUNNING` population
   (20 rows, `running_rows=20`) precisely because they are testing retry
   *count*, not per-recheck cost. Item 8 below shows the per-candidate
   recheck itself costs 34 buffers (not ~1) once the `RUNNING` population
   is 2,000 rows. No scenario in this assay exercises *both* many in-batch
   rejections *and* a large `RUNNING` population at once — that combination
   (a batch with several concurrency-blocked candidates, each triggering a
   34-buffer-class recheck) is untested, and would be the right target for
   a re-charter that specifically wants to stress the corrected mechanism's
   cost rather than its correctness.

8. **Factual correction (P2): the recheck's per-candidate cost was
   understated by 34x for the hot scenarios.** The report and
   `batch_claim.sql`'s comment cited ledger #4's own recheck cost as "~1
   buffer... regardless of key cardinality" — true only for the *idle* (0
   `RUNNING`) case. Ledger #4's own archived
   `hot_256-recheck.explain.txt`/`hot_5000-recheck.explain.txt` both show
   **34 buffers**, and `recheck_cost_diagnostic.sql` (added this round)
   re-measures the identical query directly against this apparatus's own
   schema/seed rather than only re-citing: 34 buffers at both 256 and 5,000
   distinct keys, 2,000 `RUNNING` rows each (`results/recheck_cost.explain.txt`).
   **This sharpens, rather than overturns, the cardinality-independence
   claim (L2):** the `EXPLAIN` shows why — the query is a `Bitmap Heap Scan`
   via `idx_harvest_tq_running` (`state = 'RUNNING'`, the only indexed
   predicate) reading all 2,000 `RUNNING` rows and filtering by
   `concurrency_key` in the heap (`concurrency_key` is not indexed at all
   in this schema), so cost tracks the size of the `RUNNING` **population**
   (identical at 256 and 5,000 keys, since both fixtures seed exactly 2,000
   `RUNNING` rows), not the **distinct key count** — the property L2 was
   actually testing. The two happened to be conflated in this report's
   prose because every fixture that varies key count in this apparatus
   holds `running_rows` fixed; a fixture that varied `RUNNING` population
   size independently of key count would be a cleaner test of the same
   claim, and is not this assay's own contribution to run.

**Round 4 (same PR, fourth review pass):**

9. **Reproducibility bug (P2): `schema.sql` isn't idempotent, and the new
   `tee`-based logging (item 4) made a failed rerun destructive.**
   `schema.sql`'s `CREATE TABLE` has no guard, unchanged from ledger #3/#4's
   own copy; a second `./run_assay.sh` invocation against the same database
   (exactly what the archived "Reproduce" command does if run twice without
   an intervening `dropdb`) fails immediately under `ON_ERROR_STOP` with
   `relation "harvest_task_queue" already exists` — confirmed directly by
   running it twice in a row. Combined with item 4's fix, `tee` opens
   `results/run.log` in truncate mode before `psql` produces any output, so
   the failed rerun would also have erased the previously archived log
   without producing new measurements. **Fixed:** `schema.sql` now leads
   with `DROP TABLE IF EXISTS harvest_task_queue;` (only in this assay's
   own copy — not backported to #3/#4's archived, closed copies). Verified
   directly: ran `./run_assay.sh` twice in a row against the same database
   with no intervening `dropdb`; the second run completed cleanly.

10. **Stale citation (P2): the report's equivalence-check ids no longer
    matched the archived files.** An earlier round's re-seeding for
    `recheck_cost_diagnostic.sql` (item 8) advances the shared `BIGSERIAL`
    sequence before the equivalence checks run later in the same session
    (`TRUNCATE` doesn't reset it), so the ids these checks produce shift
    between runs — the report still quoted an earlier round's `54293`/
    `64293` after a later round's archived files had moved to `78293`/
    `88293`. **Fixed:** updated to the current archived values, with a note
    that the specific ids are expected to shift between runs and only the
    control/candidate match on each line is the actual claim.

**Round 5 (same PR, fifth review pass):**

11. **Stale description (P2): the Apparatus section still described the
    removed snapshot-CTE mechanism.** Item 5 (round 2) replaced
    `claim_batched.sql`'s batch-wide `MATERIALIZED` CTE with a per-candidate
    procedural loop, and item 7 (round 3) defended grading that fix against
    the original lines — but this section's own bullet for `claim_batched.sql`
    kept describing "each batch is one query (`candidates` CTE forced
    `MATERIALIZED`...)" as if that were still the mechanism, contradicting
    the correction two sections later and making the apparatus harder to
    audit against its own description. **Fixed:** rewritten to describe the
    actual current implementation (a `FOR cand IN SELECT` fetch walked
    procedurally with the per-candidate advisory-lock recheck), with a
    pointer to item 5 for why it changed.

12. **Self-contradictory provenance (P2): "sixth run" and "the fifth run...
    its numbers are archived" can't both be true.** An earlier version of
    the Assay section's opening line said the archived numbers came from
    the apparatus's sixth run, then in the same sentence attributed them to
    the fifth run (the one that proved item 9's idempotency fix by running
    without an intervening `dropdb`) — but the sixth run is the second half
    of that same idempotency check, executed *after* the fifth against the
    database the fifth left behind, so its output — not the fifth's — is
    what overwrote `results/` and is what the report quotes. **Fixed:**
    reworded to state plainly which run produced the archived files (the
    sixth, i.e. the second of the two idempotency-check invocations) and
    why a fifth run exists at all (proving item 9, not contributing
    numbers).

## 📊 Assay

All measurements from `docs/assays/apparatus/0005-claim-batched-seek-and-refine/results/`
(`run.log`, `*.txt`, `*.explain.txt`), from the apparatus's sixth run (post
all four rounds of corrections above), one continuous psql session. The
fifth run, immediately prior in the same verification pass, existed only
to prove item 9's idempotency fix (a fresh `dropdb`/`createdb`, then
`./run_assay.sh` twice in a row with no `dropdb` in between); it produced
its own numbers, since overwritten. The sixth run — the second of that
pair, run against the already-populated database left by the fifth with no
reset — is the one whose output is on disk and quoted below.

**Buffers (L1, idle: 10,000 backlog, 4 queues, 256 keys, 0 RUNNING):**

| | buffers |
|:--|--:|
| control (committed fix) | 132 |
| candidate fetch step (`batch_claim.sql`, B=50) | 180 |
| candidate fetch, index forced (`forced_index_diagnostic.sql`) | 585 |
| candidate fetch, index forced, no `id` tiebreak (rebuttal check) | 582 |
| per-candidate recheck, idle key (0 `RUNNING`) | 1 |
| per-candidate recheck, 256 or 5,000 keys, 2,000 `RUNNING` (`recheck_cost_diagnostic.sql`) | 34 (both) |

**Wall-clock, raw `\timing` (no `EXPLAIN` instrumentation on either side, matching #4's methodology):**

| scenario | keys | running | control (`control_raw.sql`) | candidate (`claim_batched()`) | batches |
|:--|--:|--:|--:|--:|--:|
| idle_256 | 256 | 0 | 6.195 ms | 8.381 ms | 1 |
| hot_256 | 256 | 2,000 | 139.996 ms | 8.550 ms | 1 |
| hot_5000 | 5,000 | 2,000 | 971.669 ms | 5.123 ms | 1 |
| **l4_adversarial (50 poison)** | 256 | 20 | — (n/a) | **34.734 ms** | **2** |
| **l5_adversarial (200 poison)** | 256 | 20 | — (n/a) | **73.636 ms** | **5** |

(Wall-clock figures carry run-to-run noise on this box, same as every prior
ledger entry — e.g. `hot_256` control ranged 139-222ms and L4 ranged
34.7-47.4ms across this assay's six runs. Read them for order of magnitude
relative to the same run's own control, not as exactly reproducible
absolutes.)

Equivalence check: candidate claimed the identical row id to control in
every non-adversarial scenario (`results/equivalence_idle_256.txt`:
`78293`/`78293`; `equivalence_hot_256.txt`: `88293`/`88293`; hot_5000's two
raw outputs both read `22001`). The specific id values shift between runs
(round 3's re-seeding for `recheck_cost_diagnostic.sql` advances the shared
`BIGSERIAL` sequence before these two checks run later in the same
session — `TRUNCATE` does not reset it), which is why these numbers moved
from an earlier round's `54293`/`64293`; only the match between `control`
and `candidate` on each line is the actual claim. Correctness in both
adversarial scenarios was re-confirmed after the tiebreaker fix, the
authoritative-recheck fix, and the schema-idempotency fix (item 9, below):
same claimed row, same batch counts (L4: 2, L5: 5), across all four runs of
this apparatus.

**Against the lines:**

- **L1 — PASS.** 180 buffers vs. ≤300 — 1.36x the committed fix's 132, well
  inside the line and two orders of magnitude below ledger #3's 10,130-buffer
  kill. (See "Post-review corrections" above for what this number does and
  does not establish about *why* it's cheap.)
- **L2 — PASS, decisively.** 5.123ms vs. ≤160ms — **31.2x** inside the line,
  **189.6x** faster than this run's own control (971.669ms). The
  per-candidate authoritative recheck's cost stays independent of **distinct
  key count** (5,000 here vs. 256 at L1/L3) — confirmed directly at 34
  buffers in both cases (`recheck_cost_diagnostic.sql`, item 8) — because
  each recheck counts one specific key's own `RUNNING` rows via a scan
  scoped by `state = 'RUNNING'` (the only indexed predicate), not by
  `concurrency_key` (not indexed at all in this schema). That cost tracks
  the size of the `RUNNING` **population** (2,000 in both L2 and L3's
  fixtures) instead — a real property, but narrower than "cheap regardless
  of scale" until that population size is itself varied.
- **L3 — PASS, decisively.** 8.550ms vs. ≤279.99ms (2x control's
  139.996ms) — **16.4x faster than control outright**, not just inside the
  line.
- **L4 — FAIL on the batch-count sub-criterion.** Wall-clock passes cleanly
  (34.734ms vs. ≤100ms, **2.9x** inside the line) — but the shape resolved
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
  passes cleanly (73.636ms vs. ≤400ms, **5.4x** inside the line) — but the
  shape resolved in **5 batches, not the registered 4**. Same fencepost:
  `ceil((200 + 1) / 50) = 5`, not `ceil(200/50) = 4`.

**Riskiest assumption, checked first:** the risk this shape's own mechanism
introduces (per the pre-registration) was whether cost degrades linearly in
*batch count* rather than catastrophically, the way ledger #4's shape
degraded catastrophically in *attempt count*. That holds: 34.734ms at 2
batches, 73.636ms at 5 batches — a 2.5x batch-count increase producing a
2.12x wall-clock increase, consistent with cost scaling as `batches ×
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
by 2.9x-190x, and the batch-count scaling itself is linear as designed.
Four rounds of post-review correction narrow what this assay can claim
even if the batch-count lines had been written correctly: this apparatus
never established that batching bounds cost independent of backlog depth,
only that it doesn't cost meaningfully more than the (already `O(backlog)`
at this fixture depth) current committed fix, at one fixed depth. The claim
that survives, sharpened rather than intact, is the per-candidate
authoritative recheck's independence from **distinct key count** (L2) — a
real, narrower property than "cardinality-independent" first suggested,
since the same recheck's cost does scale with the size of the `RUNNING`
population (34 buffers at 2,000 rows, confirmed directly), a variable this
apparatus never varied independently of key count. Not the full "seek and
refine" story issue #1340 was named for.

**What this assay establishes, and does not:**

- Establishes: a single-round-trip-per-batch fetch, paired with the
  production path's own per-candidate `pg_try_advisory_xact_lock` +
  fresh-`COUNT` recheck (not a batch-wide snapshot — round 2's correction),
  is mechanically sound (correct row every time, including under
  adversarial saturation) and does not cost meaningfully more than the
  current fix, at this apparatus's one tested backlog depth.
- Establishes: batch-count scaling under adversarial depth is linear, not
  catastrophic — a real, positive finding about *this* mechanism's shape,
  independent of the batch-count-line fencepost bug.
- Does **not** establish: that the candidate fetch itself avoids `O(backlog)`
  scanning as backlog depth grows — untested, and the forced-index
  diagnostic (with or without this assay's own `id` tiebreak — both
  checked directly) suggests this specific query shape may not get `LIMIT`
  pushdown at any depth without further work (see the `docs/performance.md`
  #1177-baseline discrepancy noted above, which a proposed alternative
  explanation was checked against and did not survive).
- Does **not** establish: real-network round-trip cost at 2-5 batches
  (measured as 1 round trip here).
- Does **not** establish: throughput under real *concurrent* claimers
  contending for the same rows/keys — the single-session apparatus can
  exercise correct behavior for one caller at a time (which is what round
  2's fix restored) but not lock contention or throughput across several.
- Does **not** establish: cost when a batch has *both* many in-batch
  rejections (L4/L5's own territory) *and* a large `RUNNING` population
  (L2/L3's own territory) at once — no scenario here combines them, and
  item 8 shows the per-candidate recheck alone costs 34x more than this
  report first claimed once `RUNNING` reaches 2,000 rows.

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
# schema.sql is idempotent (item 9): rerunning against the same database
# without dropping it first is safe and expected to work.
PGDATABASE=prospect_assay5 ./run_assay.sh
cat results/run.log
grep "Buffers: shared hit=180" results/idle_256-batch_claim.explain.txt
grep "Index Scan using idx_harvest_tq_poll" results/idle_256-forced_index.explain.txt
grep "Sort Key" results/idle_256-forced_index_no_tiebreak.explain.txt
grep "Buffers: shared hit=34" results/recheck_cost.explain.txt
```

`schema.sql`, `seed.sql`, `seed_adversarial_50.sql`, `seed_adversarial_200.sql`,
`control.sql`, `control_raw.sql`, `batch_claim.sql`, `claim_batched.sql`,
`forced_index_diagnostic.sql`, `forced_index_no_tiebreak_diagnostic.sql`,
`recheck_cost_diagnostic.sql`, and `driver.sql` are archived alongside
`run_assay.sh` in this directory, along with the full `results/*.txt` /
`results/*.explain.txt` output and `results/run.log` (regenerated by
`run_assay.sh` itself, per the tooling fix above) this report's tables are
drawn from. No migration was added to `autumn-harvest/migrations/`; no
crate code changed. The prototype does not merge.
