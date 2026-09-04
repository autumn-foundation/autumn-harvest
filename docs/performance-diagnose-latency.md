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
  bound.** Every measured shape below, including the combined worst case,
  comes in at least an order of magnitude under the published budget.
* **The combined fan-out x fleet worst case (1000 pending activities x 1000
  live workers) is the single largest number on this page: p95 33.75 ms,
  p99 49.63 ms.** Still ~10x under budget. Measured directly, not
  extrapolated from the independent sweeps — see
  [why that distinction matters](#why-a-combined-scenario).
* **The replay path is the fastest-growing driver, but nowhere near
  dominant at these scales.** 10,001 replayed events: p95 63.82 ms — ~8x
  under budget and ~78x under the 5 s `query_timeout` that bounds it. See
  [known limitations](#known-limitations) for where this page's numbers stop
  and where growth trend, not headroom, is the caveat.
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
60 measured requests per scenario (20 for the replay-path sweep, which is
individually more expensive), preceded by 10 warmup requests (5 for the
replay-path sweep) discarded from the reported statistics. Percentiles are
nearest-rank over the measured samples, never interpolated.

## Diagnose latency vs pending-activity fan-out

Fleet fixed at 8 live workers. N=1 is the control: a single pending activity,
no fan-out and no fleet-size pressure.

| pending activities (N) | n | p50 ms | p95 ms | p99 ms | max ms |
|--:|--:|--:|--:|--:|--:|
| 1 (baseline / control) | 60 | 2.65 | 3.27 | 3.68 | 3.68 |
| 10 | 60 | 2.81 | 3.16 | 3.55 | 3.55 |
| 100 | 60 | 3.47 | 4.38 | 4.95 | 4.95 |
| 1000 | 60 | 8.53 | 9.88 | 11.32 | 11.32 |

## Diagnose latency vs live-worker fleet size

Fan-out fixed at 10 pending activities.

| live workers (M) | n | p50 ms | p95 ms | p99 ms | max ms |
|--:|--:|--:|--:|--:|--:|
| 1 | 60 | 2.92 | 3.45 | 3.85 | 3.85 |
| 10 | 60 | 3.48 | 5.04 | 5.34 | 5.34 |
| 100 | 60 | 3.91 | 4.71 | 5.13 | 5.13 |
| 1000 | 60 | 6.51 | 7.56 | 8.14 | 8.14 |

## Combined worst case: 1000 pending activities x 1000 live workers

| scenario | n | p50 ms | p95 ms | p99 ms | max ms |
|--:|--:|--:|--:|--:|--:|
| 1000 x 1000 | 60 | 31.40 | 33.75 | 49.63 | 49.63 |

### Why a combined scenario

The two sweeps above vary N and M **independently**, each holding the other
at a small fixed value. Neither alone exercises the O(N x M) shape issue
#1194 actually names — "a wide fan-out ... on a large fleet." Taken alone,
the two sweeps might suggest the combined cost extrapolates to roughly
their product or sum; measuring it directly instead of extrapolating found
p99 49.63 ms — higher than either sweep's own N=1000/M=1000 row (11.32 ms
and 8.14 ms respectively) but not their naive product, and still
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
| 21 | 20 | 4.23 | 5.12 | 5.17 | 5.17 |
| 201 | 20 | 5.24 | 5.83 | 6.03 | 6.03 |
| 2001 | 20 | 13.87 | 15.14 | 15.74 | 15.74 |
| 10001 | 20 | 58.95 | 63.82 | 64.58 | 64.58 |

## Does `p95 < 500 ms` hold?

**Yes, confirmed with a number.** Every scenario measured on this page —
including the combined fan-out x fleet worst case and the longest replay
history tested — reports p95 comfortably under the 500 ms budget, by at
least an order of magnitude in every case. Issue #809's claim stands as
measured, not merely as argued.

## Does `/diagnose` need a tighter deadline than `query_timeout`?

**No, not based on the measured range.** The replay path is the fastest
proportional grower of the three drivers, but at the largest tested history
(10,001 events — the same reference size issue #135 uses for the
CPU-path replay budget) it costs 63.82 ms p95, roughly **78x** under the 5 s
`query_timeout` that already bounds it and roughly **8x** under the 500 ms
diagnose budget. There is no evidence in this data that the existing
deadline is too loose for this endpoint specifically. See
[known limitations](#known-limitations) for the growth-trend caveat that
would justify revisiting this if a future workload pushes history length
meaningfully past what is measured here.

## Known limitations

* **Growth on the replay path trends slightly super-linear across the
  tested range**, not strictly linear in event count: 21→201 events (~10x)
  costs ~1.1x p95; 201→2001 (~10x) costs ~2.6x; 2001→10,001 (~5x) costs
  ~4.2x. The tested range stays far under budget throughout, but this page
  does not claim the trend holds at histories longer than 10,001 events —
  only that it holds up to that point, which already matches issue #135's
  own reference scale for replay cost.
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
  follow, including `claim_bench_support.rs`'s seeding approach.
* [`docs/performance-stall-diagnosis.md`](performance-stall-diagnosis.md) —
  an allocation-free instruction-count profile of the pure
  `stall_diagnosis::classify_execution` classifier this handler calls
  in-process. That page measures CPU instructions for the classifier alone;
  this page measures end-to-end wall-clock latency for the whole handler
  against a real Postgres — the two are complements, not duplicates.
