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

`valgrind --tool=callgrind --branch-sim=no --cache-sim=no`:

| | before | after | delta |
|:--|--:|--:|--:|
| Instructions (Ir) | 145,203,167 | 23,784,217 | **-83.62%** |

The realistic (2,000-pending) workload always takes the indexed path — the
small-`pending` fallback added after review only changes behavior for
`pending.len() <= 1`, which this harness never exercises — so the tiny
change from the pre-addendum 23,624,878 figure is the added dispatch branch
and the extra function-call boundary, not a regression in the indexed path
itself.

`valgrind --tool=dhat`:

| | before | after | delta |
|:--|--:|--:|--:|
| Allocated bytes | 1,630,612 | 1,781,952 | +9.3% |
| Allocated blocks | 31,021 | 31,199 | +178 |
| Bytes read | 184,163,764 | 2,758,320 | -98.5% |

Allocation count/bytes move slightly *up* (one `HashSet` build per call,
plus, after the review addendum, the harness's own oracle now materializing
150 uncovered-range name strings it previously only counted by index) —
this change is not an allocation-count win and isn't claimed as one. The
admissible evidence here is the instruction-count delta (clears the ≥5%
floor by more than an order of magnitude) and the asymptotic argument: the
nested O(pending × workers × queues-per-worker) scan is now O(pending +
workers × queues-per-worker), which is why the win *grows* with fleet size
rather than being a fixed constant-factor improvement. Bytes-read is a
strong corroborating signal (98.5% fewer memory reads, consistent with
almost all the O(n×m) string comparisons disappearing) but isn't one of the
named impact-floor categories on its own.

## 🔬 Reproduce

```sh
BIN=$(cargo bench -p autumn-harvest-plugin --no-default-features \
  --bench queue_coverage_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="queue_coverage_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=95 cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

Checking out the harness-only commit (before the algorithm change, same
commit series) and re-running reproduces the 145,203,167-instruction
baseline exactly; the current tree reproduces the 23,784,217-instruction
figure (a few thousand instructions of run-to-run noise from `rustc`/`std`
codegen details are expected and immaterial at this scale).
