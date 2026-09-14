# `queue_coverage::partition_uncovered_and_paused` — indexing the per-demand worker scan

`GET /admin/queue-coverage` (issue #774) answers a single deploy/migration
smoke-check question: *which task queues have pending work but zero live
workers polling them?* Its per-shard core, `partition_uncovered_and_paused`,
checked each pending queue name against the shard's worker fleet with
`workers.iter().any(|w| worker_covers_queue(w, &demand.queue_name,
shard_id))` — and `worker_covers_queue` itself scans that one worker's own
`queues` array looking for a match. That is an O(pending queues × workers ×
queues-per-worker) nested scan, run in full on every request to this
endpoint. This page measures and fixes it.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine (shared vCPU, wall-clock timing inadmissible). Reproduce it on your
> own hardware before designing against it — the harness is in the repo
> precisely so you can.

## 🎯 Workload

A large multi-tenant deployment is exactly the shape that stresses this
function: many independently-named queues (one, or a few, per tenant) and a
large worker fleet where each worker polls only a handful of them. The
harness, `autumn-harvest-plugin/benches/queue_coverage_profile.rs`, calls
`partition_uncovered_and_paused` directly (the real function, not a
reimplementation) with:

- 2,000 distinct pending queue names,
- 1,000 workers, each polling 10 queues drawn from a 1,850-queue "covered"
  range, deliberately leaving 150 queue names assigned to no worker at all —
  the genuinely-uncovered rows this endpoint exists to surface,
- a handful of queue names also marked paused (both inside and outside the
  covered range), so the paused-exclusion branch runs too.

Every uncovered queue forces the pre-fix `.any()` scan to run to completion
over all 1,000 workers with no covering worker to short-circuit on — the
worst case for the O(n×m) shape, and per the module's own doc comment, the
scenario this endpoint is specifically built to detect, not an adversarial
edge case.

Reproduce:

```sh
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench queue_coverage_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="queue_coverage_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=95 cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## 📈 Profile

Pre-fix `callgrind_annotate --threshold=90`:

| Ir | % | site |
|--:|--:|:--|
| 71,133,443 | 48.99% | `memcmp` (queue-name string comparisons) |
| 53,649,512 | 36.95% | inlined caller of the above (the `.any()` scan itself) |
| 2,901,077 | 2.00% | `malloc` |
| 2,254,350 | 1.55% | `shard_assignments_cover` |
| 2,132,963 | 1.47% | `free` |

The two string-comparison lines are the same nested scan, split across two
symbols by inlining under a stripped release build (`--strip=debuginfo`,
this crate's bench profile) — together **~86%** of the profile, with the
remainder almost entirely fixture setup (`malloc`/`free`). The target
dominates this workload's cost by a wide margin, well clear of the 5% floor
below which a change isn't worth making.

## 💡 Hypothesis

`shard_id` is fixed for the whole call, so whether a worker is live
(`WorkerHealth::Healthy`, `Active`/`Draining`) and assigned to that shard
never depends on which pending queue is being checked. Precomputing the
union of such workers' queue names **once** per call turns the per-demand
check from an O(workers × queues-per-worker) scan into an O(1)-average
`HashSet` lookup — collapsing the whole function from O(pending × workers ×
queues-per-worker) to O(pending + workers × queues-per-worker). This is the
same shape as `run_chain_profile`'s O(n²)→O(n) index fix (issue #1442):
replace repeated linear scans with one upfront index.

## 🔧 Change

`autumn-harvest-plugin/src/queue_coverage.rs`:

- Factored `worker_is_live_and_assigned(worker, shard_id)` out of
  `worker_covers_queue` (same logic, just the queue-independent half split
  out).
- `partition_uncovered_and_paused` now builds `covered_queues: HashSet<&str>`
  once from every live, shard-assigned worker's `queues` array, then checks
  `covered_queues.contains(demand.queue_name.as_str())` per pending queue
  instead of re-scanning `workers`.
- `worker_covers_queue` itself is untouched in behavior (same public
  predicate, same test coverage, still used as-is elsewhere) — it now calls
  the extracted helper rather than duplicating it.
- `partition_uncovered_and_paused` and `UncoveredQueueDemand` are made `pub`
  (fields included) solely so the bench binary — a separate crate — can
  call the real function on a realistic fixture, the same reason
  `dlq::group_dead_letter_rows`/`DlqRawGroup` are `pub`. Neither is part of
  the crate's HTTP-facing API.

**Review addendum (Codex, PR #1554).** Building the index is worth its
O(workers × queues-per-worker) cost only when it answers more than one
question. A `?queue_name=` filtered request narrows `pending` to at most one
row, where the pre-fix direct scan could short-circuit on the first covering
worker instead of indexing the whole fleet up front. `partition_uncovered_and_paused`
now dispatches: `pending.len() <= 1` falls back to
`partition_uncovered_and_paused_direct` (the original per-demand `.any()`
scan, kept verbatim), and only a larger `pending` builds the index. Both
paths share one `classify_pending_demand` helper for the
paused/covered/uncovered decision, so they cannot drift on that logic. A
second review finding was in the harness itself: its self-check oracle
counted paused-and-uncovered queue names from the raw `paused_indices`
array rather than the deduplicated `paused` set, so a custom
`QUEUE_COVERAGE_PROFILE_*` combination whose generated indices collide
(e.g. `PENDING=100 UNCOVERED=60 QUEUES_PER_WORKER=10 WORKERS=4`) made the
harness assert a wrong expected count against the function's own correct
output. Fixed by intersecting the deduplicated `paused` set with the
uncovered-range name set instead of counting raw indices.

**Considered and not pursued: a wider or cost-aware cutoff.** A follow-up
review round pointed out that `<= 1` does not help an unfiltered shard with,
say, two pending queues both covered by an early worker — that case still
builds the full index. The `<= 1` cutoff is not arbitrary: it is the
worst-case break-even point. Index-build cost is O(workers ×
queues-per-worker) regardless of `pending.len()`; the direct scan's
worst-case cost (every demand genuinely uncovered, no short-circuit) is
O(pending × workers × queues-per-worker), which equals the index cost at
`pending == 1` and exceeds it for any `pending > 1`. "Many genuinely
uncovered queues, no short-circuit" is this endpoint's own documented real
scenario, not a corner case, so preserving that worst-case bound is the
priority. Closing the gap the review raised (few pending, mostly covered by
an early worker) would need a genuinely cost-aware hybrid — scan directly
while counting worker-visits, and only build the index if that count
crosses what building it would have cost. That is real, additional,
stateful complexity, and there is no profiling evidence this workload shape
(small unfiltered `pending`, mostly covered) occurs on any real deployment;
the two shapes this page can point to and has measured are the
`?queue_name=`-filtered single row and the fleet-wide scan in the thousands.
Shipping the hybrid anyway would be exactly the unmeasured tuning this
agent's own charter rules out. Left as a documented trade-off rather than a
follow-up PR, pending production telemetry on `pending.len()`'s real
distribution.

Behavior is unchanged: a queue is covered iff at least one live,
shard-assigned worker lists it, exactly as before — all 35 `queue_coverage`
unit tests (including the full `worker_covers_queue` liveness/shard/queue
matrix, and two new tests for the small-`pending` direct path) pass, as
does the crate's full 1,178-test `--lib` suite.

## 📊 Measurement

**Correction (Codex review, PR #1554, second round).** The 145,203,167 /
23,624,878 figures published earlier in this PR compared two *different*
harness scaffoldings: the 145,203,167 baseline predates the review-addendum
fix to this harness's own self-check oracle (which added ~150 extra
`String` allocations to compute `paused_in_uncovered_range` correctly), so
part of that delta was harness setup cost, not the algorithm. Separately,
the indexed path's `HashSet<&str>` uses `std::collections::HashSet`'s
default per-process-random `RandomState`, so — contrary to this page's
earlier claim — its instruction count is not bit-for-bit reproducible
(see the harness's own corrected doc comment). Both numbers below are
re-measured on the *current* harness (oracle fix included) on both sides of
the algorithm change, and the indexed side is reported as an observed range
over five runs rather than a single misleadingly-precise figure.

`valgrind --tool=callgrind --branch-sim=no --cache-sim=no`, current harness
on both sides:

| | before (pre-fix algorithm) | after (indexed, 5 runs) | delta |
|:--|--:|--:|--:|
| Instructions (Ir) | 145,305,864 (identical across 5 runs — no `HashSet` on this path) | 23,780,878-23,784,259 (spread ≈0.014%) | **-83.63% to -83.64%** |

`valgrind --tool=dhat`, same before/after commits:

| | before | after | delta |
|:--|--:|--:|--:|
| Allocated bytes | 1,642,580 | 1,781,952 | +8.5% |
| Allocated blocks | 31,188 | 31,199 | +11 |
| Bytes read | 184,189,913 | ≈2,759,700 (±small) | -98.5% |

dhat's allocation count and bytes are unaffected by `RandomState`'s
per-process seed (allocation *count* does not depend on bucket layout, only
on how many entries are inserted) and were confirmed stable across repeated
runs, unlike the instruction count. Allocation count/bytes move slightly
*up* (one `HashSet` build per call, where the pre-fix code allocated
nothing extra in its hot loop) — this change is not an allocation-count win
and isn't claimed as one. The admissible evidence here is the
instruction-count delta (clears the ≥5% floor by more than an order of
magnitude, comfortably outside the ~0.014% measurement spread) and the
asymptotic argument: the nested O(pending × workers × queues-per-worker)
scan is now O(pending + workers × queues-per-worker), which is why the win
*grows* with fleet size rather than being a fixed constant-factor
improvement. Bytes-read is a strong corroborating signal (98.5% fewer
memory reads, consistent with almost all the O(n×m) string comparisons
disappearing) but isn't one of the named impact-floor categories on its
own.

## 🔬 Reproduce

```sh
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench queue_coverage_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="queue_coverage_profile") | .executable')
for i in 1 2 3 4 5; do
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
    --callgrind-out-file=cg-$i.out "$BIN" 2>&1 | grep 'I   refs'
done
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

The current tree reproduces the 23,780,878-23,784,259-instruction range on
the indexed path (run several times, per above, rather than trusting a
single sample — see the corrected doc comment in
`queue_coverage_profile.rs`). To reproduce the 145,305,864-instruction
pre-fix figure, temporarily restore `partition_uncovered_and_paused` /
`worker_covers_queue` to the single-function nested-scan form from commit
`baddbef` (the harness-only commit at the start of this PR) without
reverting `benches/queue_coverage_profile.rs` itself — the two figures in
the table above are both measured against today's harness, not against
that older commit's own (since-corrected) oracle.
