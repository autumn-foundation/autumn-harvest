# The DLQ depth gauge read every dead letter

This page follows issue #2677. `dlq::dead_letter_count` feeds the
`harvest.dlq.entries` gauge. It was an exact `COUNT(*)` over
`harvest_dead_letters`. Each worker sends it once per gauge interval, for each
assigned shard. The fix adds a counter of delta rows that triggers keep exact.
`docs/design/DESIGN-2677.md` records the decision.

## 🎯 Workload

Two captures in `autumn-harvest/tests/integration/worker_sampler_cadence_perf.rs`:

- `zz_capture_worker_sampler_statement_costs` replays 20 poll ticks of two
  workers. Each tick runs the seven gauge sampler statements, one claim and
  its completion. [`performance-gauge-sample-interval.md`](performance-gauge-sample-interval.md)
  describes the fixture.
- `zz_capture_dead_letter_write_costs` seeds only the dead letters. It then
  runs five phases through `dlq::dead_letter` and
  `dlq::discard_dead_letters_batch`:
  - 200 one-row inserts, then 200 one-row discards;
  - four 100-row discards;
  - 2,000 one-row inserts while another session holds a `REPEATABLE READ`
    snapshot open, then 5 gauge reads;
  - 8 new connections that each send 50 one-row inserts, all at once.

| size | dead letters |
|:--|--:|
| small | 10,000 |
| medium | 100,000 |
| large | 1,000,000 |

PG 16, `pg_stat_statements`, default `shared_buffers`. Evidence is buffers.
The write capture sets `pg_stat_statements.track = 'all'`. So the work of the
trigger shows as its own "nested counter work" row. A top-level row still
includes its nested buffers. The capture also reports `total_exec_time` per
call. That time is noisy on a shared VM.

`PERF_WITHOUT_COUNTER=1` drops the counter with `down.sql` before the
phases. The "before" write numbers come from that mode, with the same binary.

## 📈 Profile (before)

`dead_letter_count`, buffers per call
(`docs/perf-artifacts/worker-sampler-cadence/baseline-statement-costs.md`):

| small | medium | large |
|--:|--:|--:|
| 39 | 323 | 744,158 |

At the large size, the DLQ count was 96.5% of all buffers in that run. One
call took 145 ms (`baseline-large-explain.txt`).

## 🧭 Plan

Before (`baseline-large-explain.txt`): a parallel index-only scan over every
entry of `idx_harvest_dl_failed_at`.

After (`dlq-counter-large-explain.txt`):

```
Result  (actual rows=1)
  Buffers: shared read=1
```

`harvest_dead_letter_depth()` sums `harvest_dead_letter_counts`. That table
holds the delta rows since the last fold, so a sequential scan is the right
plan. The first call on a new connection also loads the function from the
catalog, about 150 buffers once.

## 💡 Hypothesis

A dead letter arrives only on a terminal failure. So an exact count that
each write keeps costs little per write. The gauge then reads a few rows at
any DLQ size. The count stays exact, so `harvest_dlq_growth` keeps its
meaning.

A first version updated a shared counter row. Review found three defects in
it. A long transaction elsewhere left dead versions that each writer scanned,
so the write cost grew with the flood. A `REPEATABLE READ` session default
made overlapping writers fail with 40001. And each writer needed a new grant.
The design below inserts delta rows instead.

## 🔧 Change

- Migration `20261119233037_harvest_dead_letter_counts` adds
  `harvest_dead_letter_counts` and four functions:
  `harvest_dead_letter_depth()`, `harvest_dead_letter_fold()`,
  `harvest_dead_letter_count_apply()` and `harvest_dead_letter_recount()`.
- Statement triggers run after each `INSERT` and `DELETE` with their
  transition table, and after `TRUNCATE`. One statement inserts one delta
  row, whatever its row count. A writer locks and reads no counter row.
- The timeout pass of each shard folds the delta rows once per tick. A fold
  scans the whole counter, so no writer folds. A fold takes only rows that
  no open transaction holds.
- `dead_letter_count` reads `dlq::DLQ_DEPTH_SQL`, which calls
  `harvest_dead_letter_depth()`.

## 📊 Measurement

### The gauge read

Buffers per call. Before is `baseline-statement-costs.md`, after is
`dlq-counter-statement-costs.md`:

| statement | small | medium | large |
|:--|--:|--:|--:|
| `dead_letter_count`, before | 39 | 323 | 744,158 |
| `dead_letter_count`, after | 1 | 1 | 1 |

At 1,000,000 dead letters, one call now reads 1 buffer in 0.11 ms. Per worker
and assigned shard, at the 15 s gauge interval, that is 240 buffers per hour
in place of 179 M.

### The writes

Buffers per call. Before is `write-before-write-costs.md`, after is
`write-after-write-costs.md`:

| statement | small before | small after | large before | large after |
|:--|--:|--:|--:|--:|
| insert, one row | 9.3 | 11.3 | 11.5 | 13.6 |
| discard, one row | 5.0 | 8.6 | 6.0 | 9.6 |
| discard, 100 rows | 412 | 515 | 594 | 696 |
| insert, one row, snapshot held | 9.3 | 12.3 | 11.3 | 14.3 |
| insert, 8 connections at once | 12.0 | 16.9 | 14.2 | 19.1 |

Time per call, same files:

| statement | small before | small after | large before | large after |
|:--|--:|--:|--:|--:|
| insert, one row | 0.020 ms | 0.051 ms | 0.026 ms | 0.055 ms |
| discard, one row | 0.005 ms | 0.032 ms | 0.007 ms | 0.042 ms |
| discard, 100 rows | 0.115 ms | 0.207 ms | 0.308 ms | 0.416 ms |

- The nested counter work is the delta insert: 2.0 buffers for an insert and
  2.6 for a one-row discard, at every size.
- A delete copies each old row into its transition table. That reads the
  heap page once more: about 1 buffer per deleted row.
- A one-row call costs 0.03 ms more. Most of that is the switch of
  `search_path` that each `SECURITY DEFINER` call makes.
- The 8 new connections each load the trigger function from the catalog
  once. That is most of their extra buffers.

### A long transaction during a flood

2,000 one-row inserts while another session holds a snapshot open. Vacuum
then keeps each dead row version, as during a long `pg_dump`:

| | small before | small after | large before | large after |
|:--|--:|--:|--:|--:|
| buffers per insert | 9.3 | 12.3 | 11.3 | 14.3 |
| buffers per gauge read | 283 | 33 | 743,679 | 25 |
| counter table pages | — | 18 | — | 18 |

A writer only inserts. It never reads the counter, so its cost stays flat
however long the flood runs. The harness runs no timeout pass, so its
counter held 2,808 unfolded rows here. In a worker, the timeout pass folds
them each tick, off the write path.

### Concurrency

- `open_writers_never_wait_for_the_counter` holds 12 open writers and a
  delete. No insert waits.
- `writers_above_read_committed_never_conflict_on_the_counter` runs two
  overlapping writers at `REPEATABLE READ` and at `SERIALIZABLE`, after
  other writers committed. Both commit, and the count is exact. The first
  version failed it with 40001.

## ✅ The alert

`dlq_gauge_growth_of_six_fires_the_growth_alert` runs a real worker with a
recording metrics sink. It inserts 6 dead letters. The emitted series grows
by 6, so `max_over_time - min_over_time > 5` fires, as `harvest_dlq_growth`
needs.

## Follow-ups

- One elected sampler per shard (#2679, PR #2750) now cuts the calls of
  every gauge sampler by the worker count. The DLQ read no longer needed it,
  and it costs 1 buffer when it runs.

## 🔬 Reproduce

```sh
PERF_LABEL=dlq-counter HARVEST_TEST_DATABASE_URL=postgres://… cargo test -p autumn-harvest \
  --test integration -- --ignored --exact \
  worker_sampler_cadence_perf::zz_capture_worker_sampler_statement_costs --nocapture
PERF_LABEL=write-after HARVEST_TEST_DATABASE_URL=postgres://… cargo test -p autumn-harvest \
  --test integration -- --ignored --exact \
  worker_sampler_cadence_perf::zz_capture_dead_letter_write_costs --nocapture
PERF_WITHOUT_COUNTER=1 PERF_LABEL=write-before HARVEST_TEST_DATABASE_URL=postgres://… \
  cargo test -p autumn-harvest --test integration -- --ignored --exact \
  worker_sampler_cadence_perf::zz_capture_dead_letter_write_costs --nocapture
```
