# Diagnose latency: measuring issue #809's unverified p95 < 500 ms claim

Issue #809 published `GET /api/harvest/workflows/{id}/diagnose` (shipped in
PR #1188) with a `p95 < 500 ms` success metric, argued structurally
(single-shard read, narrow projection, replay bounded by `query_timeout`) and
never measured. The argument has a real hole: the handler's cost is not
constant in the shape of the execution being diagnosed. Three drivers are
unbounded — pending-activity fan-out width (N) crossed against live-worker
fleet size (M) in an O(N x M) in-memory eligibility fold, the deliberately
unbounded pending-row count feeding it, and a full history replay
(`build_awaitables_report`) on the `sleeping_timer`/`no_pending_work` path,
bounded only by `WorkerConfig::query_timeout` (default 5 s — 10x the
published budget). See issue #1194 for the full argument.

> **These are starter reference numbers, not an SLO.** They were taken on one
> machine with one Postgres configuration (below). Reproduce them on your own
> hardware before designing against them.

## TL;DR

* **`p95 < 500 ms` holds, confirmed with numbers — not replaced with a
  bound.** Every measured shape below clears the budget; margins range from
  ~8x (the longest replay history) to over 100x (the single-activity
  baseline).
* **The replay path produces the largest numbers on this page and the
  smallest margin.** 10,001 replayed events: p95 61.06 ms, max 65.04 ms —
  only ~8x under the 500 ms budget, and ~82x under the 5 s `query_timeout`
  that bounds it. Cost grows **linearly** in event count here (see
  [known limitations](#known-limitations) for the slope), not
  super-linearly — an earlier draft of this page mischaracterized the
  trend by comparing ratios of totals instead of incremental slopes, which
  hides a large fixed per-request overhead and makes a linear trend look
  like it's accelerating.
* **The combined fan-out x fleet worst case (1000 pending activities x 1000
  live workers) is the largest number driven by the O(N x M) eligibility
  fold specifically: p95 34.44 ms, p99 34.87 ms — ~14x under budget**, and
  measured directly rather than extrapolated from the independent sweeps —
  see [why that distinction matters](#why-a-combined-scenario). It is not
  the largest number on the page overall; the replay path's is.
* **No tighter per-request deadline than `query_timeout` (5 s default) is
  warranted for this endpoint specifically**, based on the measured range —
  see [the query_timeout question](#does-diagnose-need-a-tighter-deadline-than-query_timeout).

## Reference environment

```bash
cargo bench -p autumn-harvest-plugin --bench diagnose_bench
```

Requires a reachable Postgres: either `HARVEST_TEST_DATABASE_URL` (an admin
connection string — a throwaway database is created, migrated and dropped for
the run) or a working Docker daemon (a `postgres:16` testcontainer). With
neither, the bench prints a skip notice and exits 0.

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers` |
| Profile | `bench` (release) |
| Harness | `autumn-harvest-plugin/benches/diagnose_bench.rs` |

Every table on this page is from one run, taken together, on that machine.
120 measured requests per scenario, preceded by 15 warmup requests discarded
from the reported statistics. 120 (not the more typical 60/20) is
deliberate: nearest-rank p99 is `ceil(0.99n)`, which equals `n` itself —
i.e. p99 collapses to a relabeled `max` — for any `n < 100`, matching the
`MIN_MEANINGFUL_SAMPLES` floor `claim_bench_support.rs` establishes for the
same reason. Every row below is comfortably above that floor, so p99 is a
real (if still coarse — the closest-to-worst observation, not an
interpolated statistic) distinct percentile in every table on this page, not
`max` under a different name. Percentiles are nearest-rank, never
interpolated.

## Diagnose latency vs pending-activity fan-out

Fleet fixed at 8 live workers. N=1 is the control: a single pending activity,
no fan-out and no fleet-size pressure.

| pending activities (N) | n | p50 ms | p95 ms | p99 ms | max ms |
|--:|--:|--:|--:|--:|--:|
| 1 (baseline / control) | 120 | 2.70 | 3.69 | 5.73 | 7.31 |
| 10 | 120 | 2.62 | 3.11 | 3.31 | 3.41 |
| 100 | 120 | 3.05 | 3.64 | 3.95 | 4.32 |
| 1000 | 120 | 7.65 | 10.30 | 11.78 | 12.07 |

## Diagnose latency vs live-worker fleet size

Fan-out fixed at 10 pending activities.

| live workers (M) | n | p50 ms | p95 ms | p99 ms | max ms |
|--:|--:|--:|--:|--:|--:|
| 1 | 120 | 3.01 | 3.74 | 4.51 | 4.72 |
| 10 | 120 | 2.93 | 3.63 | 4.66 | 4.88 |
| 100 | 120 | 3.33 | 3.90 | 4.04 | 4.69 |
| 1000 | 120 | 6.07 | 6.69 | 7.40 | 7.52 |

## Combined worst case: 1000 pending activities x 1000 live workers

| scenario | n | p50 ms | p95 ms | p99 ms | max ms |
|--:|--:|--:|--:|--:|--:|
| 1000 x 1000 | 120 | 31.62 | 34.44 | 34.87 | 34.97 |

### Why a combined scenario

The two sweeps above vary N and M **independently**, each holding the other
at a small fixed value. Neither alone exercises the O(N x M) shape issue
#1194 actually names — "a wide fan-out ... on a large fleet." Taken alone,
the two sweeps might suggest the combined cost extrapolates to roughly
their product or sum; measuring it directly instead of extrapolating found
p99 34.87 ms — higher than either sweep's own N=1000 or M=1000 row (11.78 ms
and 7.40 ms respectively) but far short of their naive product, and still
comfortably within budget. `required_capabilities` is unset on every
seeded task/worker in this harness, so `eligible_worker_ids`' per-worker
JSON-deserialize branch is never exercised here — see
[known limitations](#known-limitations).

## Diagnose latency on the replay path

No pending activities, so the DB-observable categories resolve to
`no_pending_work` and the handler drives `build_awaitables_report` — a full
history replay bounded by `WorkerConfig::query_timeout` (default 5 s).
Fleet fixed at 8 live workers (irrelevant on this path: eligibility is only
computed for pending-activity/workflow task rows, and this scenario seeds
neither).

| history events replayed | n | p50 ms | p95 ms | p99 ms | max ms |
|--:|--:|--:|--:|--:|--:|
| 21 | 120 | 3.20 | 3.70 | 3.79 | 4.35 |
| 201 | 120 | 4.25 | 4.73 | 5.05 | 5.56 |
| 2001 | 120 | 12.49 | 15.12 | 16.76 | 17.14 |
| 10001 | 120 | 54.75 | 61.06 | 62.49 | 65.04 |

## Does `p95 < 500 ms` hold?

**Yes, confirmed with a number.** Every scenario measured on this page —
including the combined fan-out x fleet worst case and the longest replay
history tested — reports p95 under the 500 ms budget. The margin varies by
shape: over 100x at the baseline control, down to ~8x at the longest
replay history (10,001 events) — see
[the query_timeout question](#does-diagnose-need-a-tighter-deadline-than-query_timeout)
for that shape specifically. Issue #809's claim stands as measured, not
merely as argued.

## Does `/diagnose` need a tighter deadline than `query_timeout`?

**No, not based on the measured range.** The replay path produces the
largest numbers on this page, but at the largest tested history (10,001
events — the same reference size issue #135 uses for the CPU-path replay
budget) it costs 61.06 ms p95, roughly **82x** under the 5 s
`query_timeout` that already bounds it and roughly **8x** under the 500 ms
diagnose budget. There is no evidence in this data that the existing
deadline is too loose for this endpoint specifically. See
[known limitations](#known-limitations) for the growth-trend caveat that
would justify revisiting this if a future workload pushes history length
meaningfully past what is measured here.

## Known limitations

* **Growth on the replay path is linear in event count across the tested
  range, once fixed per-request overhead is accounted for** — not
  super-linear. The incremental slope between each pair of adjacent sweep
  points is nearly constant: 21→201 events costs 0.00572 ms/event,
  201→2001 costs 0.00577 ms/event, 2001→10,001 costs 0.00574 ms/event.
  (An earlier draft of this page compared ratios of *total* latency across
  10x/10x/5x jumps in event count instead of these incremental slopes,
  which made a linear trend look like it was accelerating — the ratios
  grow because a roughly constant ~3.6 ms fixed cost per request is a
  shrinking fraction of the total as event count grows, not because the
  per-event marginal cost is increasing.) This page does not claim the
  linear trend holds at histories longer than 10,001 events — only that it
  holds up to that point, which already matches issue #135's own reference
  scale for replay cost.
* **The combined worst-case scenario uses tasks and workers with no
  `required_build_id`, `required_capabilities`, or worker sessions set.**
  `eligible_worker_ids` has additional per-candidate branches for those
  (a `serde_json` deserialize of worker labels when capabilities are
  required, a `BuildCompatibilitySet` lookup for build routing) that this
  page's combined scenario does not exercise. A fan-out where most rows
  carry `required_capabilities` would cost more per candidate than measured
  here.
* **`KEY_CARDINALITY`-style contention effects don't apply.** Unlike
  `claim_task`, `/diagnose` takes no locks and performs no writes, so there
  is no analogue of `claim_bench`'s concurrent-claimer contention to model;
  every number on this page is a single sequential requester.
* **One machine, one Postgres configuration** — see
  [reference environment](#reference-environment).

## See also

* `autumn-harvest-plugin/benches/diagnose_bench.rs` — the harness this page's
  numbers come from.
* [`docs/performance.md`](performance.md) — the `claim_task`/enqueue
  benchmark (issue #786) this bench's harness conventions and report format
  follow, including `claim_bench_support.rs`'s seeding approach and its
  `MIN_MEANINGFUL_SAMPLES` floor.
* [`docs/performance-stall-diagnosis.md`](performance-stall-diagnosis.md) —
  an allocation-free instruction-count profile of the pure
  `stall_diagnosis::classify_execution` classifier this handler calls
  in-process. That page measures CPU instructions for the classifier alone;
  this page measures end-to-end wall-clock latency for the whole handler
  against a real Postgres — the two are complements, not duplicates.
