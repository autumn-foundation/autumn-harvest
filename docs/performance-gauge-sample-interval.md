# The database gauge samplers ran on the poll interval

This page follows issue #2668. Six gauge samplers, with seven statements, read
the database on each 500 ms poll tick of each worker. The stranded-work query also had a plan
defect. The fix rewrites that query and moves every database gauge sampler to
`WorkerConfig::gauge_sample_interval`, 15 s by default.
`docs/design/DESIGN-gauge-sample-interval.md` records the decision.

## 🎯 Workload

| sampler | function |
|:--|:--|
| `spawn_queue_depth_sampler` | `bounded_queue_depths`, `oldest_pending_ages` |
| `spawn_concurrency_sampler` | `concurrency_key_stats` |
| `spawn_rate_limit_sampler` | `sample_rate_limit_buckets` |
| `spawn_dlq_depth_sampler` | `dead_letter_count` |
| `spawn_queue_pause_sampler` | `paused_queue_names` |
| `spawn_stranded_work_sampler` | `claimable_pending_demand_by_queue` |

`autumn-harvest/tests/integration/worker_sampler_cadence_perf.rs` replays 20
poll ticks of two workers. One tick runs the seven statements and one claim
with its completion. The fixture reuses the backlog of `queue_age_gauge_perf`:
20 queues with a cubic skew, concurrency-capped and rate-limited rows, and
terminal rows. It adds dead letters with 0.3 to 1 kB payloads, and rate-limit
buckets that are 90% per-tenant keys.

| size | pending rows | dead letters | buckets |
|:--|--:|--:|--:|
| small | 10,000 | 10,000 | 100 |
| medium | 100,000 | 100,000 | 1,000 |
| large | 300,000 | 1,000,000 | 10,000 |

PG 16, `pg_stat_statements`, default `shared_buffers`. Evidence is calls and
buffers, not wall-clock.

## 📈 Profile (before)

Buffers per call (`docs/perf-artifacts/worker-sampler-cadence/baseline-statement-costs.md`):

| statement | small | medium | large |
|:--|--:|--:|--:|
| `bounded_queue_depths` | 762 | 1,251 | 2,013 |
| `oldest_pending_ages` | 2,674 | 1,614 | 2,161 |
| `concurrency_key_stats` | 672 | 6,794 | 20,230 |
| `sample_rate_limit_buckets` | 2 | 14 | 132 |
| `dead_letter_count` | 39 | 323 | 744,158 |
| `paused_queue_names` | 0 | 0 | 0 |
| `claimable_pending_demand_by_queue` | 22,801 | more than 300 s | not run |
| claim and completion (workload) | 120 | 135 | 191 |

- At the small size the stranded-work query was 80% of all buffers. Its
  figure comes from `explain-stranded-old-small.txt`, because the baseline
  run left the statement out.
- At the medium size one stranded-work call ran for more than 300 s. The
  baseline left that statement out at the medium and large sizes
  (`PERF_SKIP_STRANDED=1`).
- At the large size the DLQ count was 96.5% of all buffers. It is a parallel
  index-only scan over every dead letter.

## 🧭 Plan

The stranded-work query at the medium size, before the fix
(`plan-stranded-old-medium.txt`, estimated rows):

```
Nested Loop Left Join  (Join Filter: e.id = tq.workflow_exec_id)
  ->  Merge Anti Join  (rows=1)   -- harvest_queue_pauses
        ->  Index Scan using idx_harvest_tq_claim_seek on harvest_task_queue tq  (rows=92939)
  ->  Seq Scan on harvest_workflow_executions e  (rows=50000)
```

The planner estimates one row out of the queue-pause anti-join. So it scans
all 50,000 executions for each of about 93,000 pending rows. The empty pause
tables have no statistics, and autovacuum never analyzes them.

After the fix the `PAUSED` gate is a hashed subplan over
`idx_harvest_we_paused`. The same call reads 114,965 buffers in 0.23 s
(`explain-stranded-new-medium.txt`). The old query was cancelled at 300 s
(`explain-stranded-old-medium.txt`). At the small size the old query read
22,801 buffers and the new one 9,704 (`explain-stranded-*-small.txt`). That is
57% fewer. EXPLAIN counts differ a little from the per-call table below.

## 💡 Hypothesis

1. The `LEFT JOIN` on the executions gives the planner a quadratic plan. A
   `NOT EXISTS` probe for a `PAUSED` run gives the same rows with one index
   probe.
2. Each statement is an exact count that a gauge needs. Prometheus scrapes a
   gauge each 15 to 60 s, so no scrape reads most samples at 500 ms.

## 🔧 Change

- `claimable_pending_demand_query` probes for a `PAUSED` run with
  `NOT EXISTS`.
- `spawn_gauge_samplers` starts every database gauge sampler on
  `gauge_sample_interval`. That includes the two population samplers of
  issue #2668.

## 📊 Measurement

Buffers per call after the fix
(`docs/perf-artifacts/worker-sampler-cadence/after-statement-costs.md`). Only
the stranded-work query changes per call:

| statement | small | medium | large |
|:--|--:|--:|--:|
| `claimable_pending_demand_by_queue` | 9,681 | 114,943 | 448,314 |

Buffers per worker per hour for the seven statements. Before is 7,200 calls
at 500 ms. After is 240 calls at 15 s:

| size | before | after | change |
|:--|--:|--:|--:|
| small | 194.0 M | 3.3 M | −98.3% |
| medium | 72.0 M, plus stranded-work calls of more than 300 s | 30.0 M | — |
| large | 5.53 B, plus stranded-work calls of more than 300 s | 292 M | — |

A worker with a sharded pool runs the fleet-wide samplers once for each shard,
so multiply by the shard count.

`stranded_demand_matches_the_reference_query` proves that the rewrite returns
the same groups as the old query. `gauge_samplers_wait_for_the_gauge_interval_not_the_poll_interval`
counts real connections. No gauge sampler opens one before the gauge interval
ends. The test fails when a single sampler uses `poll_interval`.

## Follow-ups

- At 1,000,000 dead letters one DLQ count reads 744,158 buffers. A counter
  table would make it constant, but it adds a write to each dead letter.
- The stranded-work query still reads each claimable pending row. One elected
  sampler per shard would cut the calls by the worker count.
- `concurrency_key_stats` scans the whole task queue. No index covers its
  PENDING rows.

## 🔬 Reproduce

```sh
PERF_LABEL=after HARVEST_TEST_DATABASE_URL=postgres://… cargo test -p autumn-harvest \
  --test integration -- --ignored --exact \
  worker_sampler_cadence_perf::zz_capture_worker_sampler_statement_costs --nocapture
```

Set `PERF_SKIP_STRANDED=1` to leave the stranded-work statement out.

The old and new stranded-work queries are `stranded-old.sql` and
`stranded-new.sql` in the artifact directory. Run each with `psql` on a
database that the harness seed has filled.
