# The DLQ depth gauge read every dead letter

This page follows issue #2677. `dlq::dead_letter_count` feeds the
`harvest.dlq.entries` gauge. It was an exact `COUNT(*)` over
`harvest_dead_letters`. Each worker sends it once per gauge interval, for each
assigned shard. The fix adds a counter table that triggers keep exact.
`docs/design/DESIGN-2677.md` records the decision.

## 🎯 Workload

Two captures in `autumn-harvest/tests/integration/worker_sampler_cadence_perf.rs`:

- `zz_capture_worker_sampler_statement_costs` replays 20 poll ticks of two
  workers. Each tick runs the seven gauge sampler statements, one claim and
  its completion. [`performance-gauge-sample-interval.md`](performance-gauge-sample-interval.md)
  describes the fixture.
- `zz_capture_dead_letter_write_costs` seeds only the dead letters. It then
  measures 200 one-row inserts through `dlq::dead_letter`, 200 one-row
  discards and four 100-row discards through `dlq::discard_dead_letters_batch`,
  and 8 connections that each insert 50 rows at once.

| size | dead letters |
|:--|--:|
| small | 10,000 |
| medium | 100,000 |
| large | 1,000,000 |

PG 16, `pg_stat_statements`, default `shared_buffers`. Evidence is buffers.
The write capture also reports `total_exec_time` per call. That time is
noisy on a shared VM, so read it as a range.

## 📈 Profile (before)

`dead_letter_count`, buffers per call
(`docs/perf-artifacts/worker-sampler-cadence/after-statement-costs.md`):

| small | medium | large |
|--:|--:|--:|
| 39 | 323 | 744,158 |

At the large size the DLQ count was 96.5% of all sampler buffers. One call
took 145 ms.

## 🧭 Plan

Before (`after-large-explain.txt`): a parallel index-only scan over every
entry of `idx_harvest_dl_failed_at`.

After (`dlq-counter-large-explain.txt`):

```
Aggregate  (actual rows=1)
  Buffers: shared read=1
  ->  Seq Scan on harvest_dead_letter_counts  (actual rows=1)
```

The counter table holds one row per concurrent writer at most. A sequential
scan is the right plan for it.

## 💡 Hypothesis

A dead letter arrives only on a terminal failure. So an exact count that
each write keeps costs little per write. The gauge then reads a few rows at
any DLQ size. The count stays exact, so the `harvest_dlq_growth` alert keeps
its meaning.

## 🔧 Change

- Migration `20261119233037_harvest_dead_letter_counts` adds
  `harvest_dead_letter_counts`, `harvest_dead_letter_count_apply()` and
  `harvest_dead_letter_recount()`.
- Statement triggers run after each `INSERT` and `DELETE` with their
  transition table, and after `TRUNCATE`. One statement writes the counter
  once, whatever its row count.
- A writer locks a free counter row with `FOR UPDATE SKIP LOCKED`, or inserts
  a row. It never waits for another writer.
- `dead_letter_count` reads `dlq::DLQ_DEPTH_SQL`, the sum of the counter.

## 📊 Measurement

### The gauge read

Buffers per call
(`docs/perf-artifacts/worker-sampler-cadence/dlq-counter-statement-costs.md`):

| statement | small | medium | large |
|:--|--:|--:|--:|
| `dead_letter_count`, before | 39 | 323 | 744,158 |
| `dead_letter_count`, after | 1 | 1 | 1 |

At 1,000,000 dead letters, one call now reads 1 buffer in 0.04 ms. Per worker
per hour at the 15 s gauge interval, that is 240 buffers in place of
179 M.

### The writes

Buffers per call and ms per call. Before is
`write-before-write-costs.md`, after is `write-after-write-costs.md`:

| statement | size | buffers before | buffers after | ms before | ms after |
|:--|:--|--:|--:|--:|--:|
| insert, one row | small | 9.3 | 15.3 | 0.029 | 0.047 to 0.103 |
| insert, one row | large | 11.6 | 17.5 | 0.041 | 0.058 to 0.105 |
| discard, one row | small | 5.0 | 12.0 | 0.010 | 0.044 to 0.046 |
| discard, one row | large | 6.0 | 13.0 | 0.010 | 0.033 to 0.053 |
| discard, 100 rows | small | 412 | 518 | 0.220 | 0.166 to 0.237 |
| discard, 100 rows | large | 594 | 699 | 0.425 | 0.375 to 0.397 |
| insert, 8 connections | large | 14.1 | 25.5 to 26.1 | 0.081 | 0.129 to 0.217 |

The "ms after" ranges come from two runs.

- The nested counter update reads 6 buffers per statement at every size. The
  capture reports it as its own row.
- A delete also copies each old row into its transition table. That reads
  the heap page once more: 1 buffer per deleted row.
- With 8 writers at once, each insert reads about 11 buffers more. The
  writers share the page of the counter table.
- No insert waited for another writer.
  `open_writers_never_wait_for_the_counter` proves it: 12 open transactions
  each insert at once. With `SKIP LOCKED` removed, the second writer waits
  and the test fails.

A dead-letter write is part of a failure transaction that also writes the
task, the history and the run. So the added cost is small in that context.
The DLQ grows only on a terminal failure.

## ✅ The alert

`dlq_gauge_growth_of_six_fires_the_growth_alert` runs a real worker with a
recording metrics sink. It inserts 6 dead letters. The emitted series grows
by 6, so `max_over_time - min_over_time > 5` fires, as `harvest_dlq_growth`
needs.

## Follow-ups

- One elected sampler per shard (#2679) would cut the calls of the other
  samplers by the worker count. The DLQ read no longer needs it.

## 🔬 Reproduce

```sh
PERF_LABEL=dlq-counter HARVEST_TEST_DATABASE_URL=postgres://… cargo test -p autumn-harvest \
  --test integration -- --ignored --exact \
  worker_sampler_cadence_perf::zz_capture_worker_sampler_statement_costs --nocapture
PERF_LABEL=write-after HARVEST_TEST_DATABASE_URL=postgres://… cargo test -p autumn-harvest \
  --test integration -- --ignored --exact \
  worker_sampler_cadence_perf::zz_capture_dead_letter_write_costs --nocapture
```

Run the write capture on a build without the migration, with
`PERF_LABEL=write-before`, for the baseline.
