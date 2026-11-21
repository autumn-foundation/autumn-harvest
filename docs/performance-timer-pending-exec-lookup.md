# Open-timer lookups of a long run (issue #2643)

Each workflow decision reads the open timers of its run from
`harvest_timers`. Three functions in `autumn-harvest/src/worker.rs` send these
reads:

| Function | Read | When |
|---|---|---|
| `worker::ingest_due_timers_and_signals` | the due open timers of the run | each decision |
| `worker::persist_started_timer` and `worker::suspended_command_event_count` | one open timer by `timer_id` | each timer command |
| `worker::load_unfired_timers_by_id` | the open timers in a list of ids | each batch of timer commands |

Each read filters on `workflow_exec_id` and `fired`. A fired timer row stays
until retention deletes its run. Before issue #2643, the only index by
execution was `idx_harvest_timers_exec (workflow_exec_id)`. So each read
fetched every timer that the run had fired, and dropped it in a filter. The
cost grew with the length of the run.

Two migrations now give `idx_harvest_timers_exec` the shape
`(workflow_exec_id, fired)`. A read reaches the open timers directly. The
cost per read is flat.

> **This is a reference measurement, not an SLO.** It comes from one machine
> with one Postgres configuration. Reproduce it before you design against it.

## TL;DR

* **Timer-lookup buffers fall 47%, 66% and 78%** for runs of 250, 1,000 and
  2,000 timers. Before, the lookups were 9.7%, 13.1% and 20.4% of all
  buffers of the run. Tool: `pg_stat_statements`, worker-driven run.
* **Buffers per lookup are flat at 4.0.** Before, they were 7.6, 11.6 and
  18.2. The old cost grew with the run. The new cost does not.
* **On a 10,000-timer run with its rows spread over the heap,** one lookup
  falls from 6,962 buffers and 10,040 index rows to 4 buffers and 1 row.
* **The write cost is within noise.** WAL per run changes by +0.3% to +0.7%.
  A fresh build on 1.13 million rows is 5.7% larger: 9,142,272 bytes against
  8,650,752.
* **Results are identical.** The timer digests of each run and five
  full-result hashes on the fixture match before and after.

## Why the old index could not help

`fired` changes in place. The run keeps each fired row. The old index cannot
tell the open timers (usually 0 to 2) from the fired ones (up to about 16,000
at the 50,000-event history cap). The planner has one other index:
`idx_harvest_timers_pending (fires_at) WHERE NOT fired`. That index covers the
whole shard. So the old cost was about the smaller of two numbers: the timers
of the run, and the open timers of the shard. Both grow without a bound.

Diesel binds `fired` as a parameter. A cached generic plan therefore cannot
use a partial index on `NOT fired`. A test of the partial index
`(workflow_exec_id, timer_id) WHERE NOT fired` confirms this: the generic plan
fell back to 6,962 buffers. A plain key column on `fired` serves both plan
modes.

`(workflow_exec_id, fired, timer_id)` serves both modes too, but it is 55 MB
on the fixture against 8.7 MB. The unique `timer_id` stops the B-tree from
merging duplicate keys. Added next to the old index, it also raised the WAL
of each fire update by about 35%. It was rejected.

## The workload

### Worker-driven capture

`autumn-harvest/tests/integration/timer_pending_exec_lookup_perf.rs` seeds the
fixture below. It then starts one `timer_loop` run through
`start_or_load_workflow_execution`. The run fires `n` zero-second timers in a
row, one decision each. The worker drives the run to completion. The capture
reads `pg_stat_statements` for the database and explains the three lookups on
the finished run. Each size gets a fresh, migrated database.

### Fixture

[`perf-artifacts/timer-pending-exec-lookup/seed.sql`](perf-artifacts/timer-pending-exec-lookup/seed.sql)
is seeded and deterministic:

* 20,000 runs: 19,000 short runs with 0 to 4 timers, 900 medium runs with 100
  timers and 100 long runs with 10,000 timers.
* 1,130,000 timer rows. 98.4% have fired. Each run with timers has one open
  timer in the future.
* 2,000 due timers have not fired yet. This is an ingest lag.
* The rows go in in time order, so the timers of one run spread over the heap.
* One day of fire updates is not vacuumed. The table has 4,226 dead tuples.

The worker-driven run writes its own timers. Only one run writes, so its rows
sit in adjacent heap pages. That understates the heap cost of a production
run, whose rows mix with the rows of other runs. The static fixture plans
below measure that case.

## Profile

Run of 2,000 timers, before the change (tool: `pg_stat_statements`):

| Rank by buffers | Statement | Buffers | % of buffers | Calls |
|--:|---|--:|--:|--:|
| 1 | `INSERT INTO harvest_events` | 75,731 | 14.2 | 4,001 |
| 2 | **open timer by `timer_id`** | 72,604 | 13.6 | 4,000 |
| 3 | a claim statement (`WITH worker_info AS ...`) | 58,960 | 11.0 | 2,001 |
| 4 | the claim `UPDATE` of `harvest_task_queue` | 49,269 | 9.2 | 2,000 |
| 5 | **due open timers of the run** | 36,320 | 6.8 | 2,001 |

An earlier baseline run gave 108,417 timer-lookup buffers, 0.5% off the
committed 108,924. Total buffers vary more between runs, because idle polls
vary. `baseline-capture.txt` has the full top-10 rankings by buffers, calls
and WAL bytes.

The two timer lookups are 20.4% of the buffers of the run and 3.6% of its
calls. No temp blocks were written. COMMIT, `SELECT $1`, BEGIN and the
`to_regclass` probe lead the calls ranking. [Issue #2518](https://github.com/autumn-foundation/autumn-harvest/issues/2518)
already recorded them.

## Measurement

### Worker-driven runs

| Run | Lookup buffers before | after | Delta | Buffers per call before | after | Share of buffers before | after |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 250 timers | 5,689 | 3,014 | -47.0% | 7.6 | 4.0 | 9.7% | 5.4% |
| 1,000 timers | 34,899 | 12,017 | -65.6% | 11.6 | 4.0 | 13.1% | 5.4% |
| 2,000 timers | 108,924 | 24,019 | -78.0% | 18.2 | 4.0 | 20.4% | 5.3% |

| Run | Total buffers before | after | WAL bytes before | after | Calls before | after |
|--:|--:|--:|--:|--:|--:|--:|
| 250 timers | 58,857 | 56,008 | 990,652 | 997,535 | 20,897 | 20,994 |
| 1,000 timers | 266,175 | 222,936 | 4,004,839 | 4,018,181 | 82,976 | 84,813 |
| 2,000 timers | 534,871 | 453,505 | 7,998,552 | 8,034,542 | 164,768 | 164,637 |

The lookup calls are 751, 3,001 and 6,001 in both runs. The statement count
does not change. On the finished run of 2,000 timers, a generic-plan lookup
reads 2,492 index rows and 32 buffers before. After, `fired` is an index
condition, and the lookup reads 0 rows and 3 buffers.

### Static fixture, 10,000-timer run

`measure.sql` prepares each lookup as Diesel sends it. It explains each lookup
in both plan-cache modes, for a long, a medium and a short run.

| Run | Statement | Before buffers | Before index rows | After buffers | After index rows |
|---|---|--:|--:|--:|--:|
| long (10,000 timers) | due open timers | 6,962 | 10,040 | 4 | 1 |
| long | open timer by id | 6,934 | 10,040 | 4 | 1 |
| long | open timers by ids | 6,962 | 10,040 | 4 | 1 |
| medium (100) | each of the three | 103 | 100 | 4 | 1 |
| short (4) | each of the three | 7 | 4 | 4 | 1 |

These are generic plans. With custom plans, the long run falls from 1,935,
1,950 and 2,143 buffers to 10, 4 and 7.

### The plan shape across three shard sizes

`auto-mode.sql` runs the by-id lookup six times, then explains it on the long
run. `grow.sql` adds sleeping runs, each with one open timer.

| Open timers on the shard | Before | After |
|--:|--:|--:|
| 18,200 | 1,950 (walk of the shard-wide open-timer index) | 4 |
| 118,200 | 6,934 (all timers of the run) | 4 |
| 1,018,200 | 6,934 (all timers of the run) | 4 |

## Write cost

`write.sql` inserts 10,000 timers one row at a time, then fires each one. It
runs twice after a `CHECKPOINT` (tool: `pg_current_wal_lsn()` deltas).

| Index on `harvest_timers` | Insert WAL per timer | Fire WAL per timer | Index bytes after the writes |
|---|--:|--:|--:|
| `(workflow_exec_id)`, before | 3,064 / 3,088 | 358 / 338 | 8,798,208 / 8,937,472 |
| `(workflow_exec_id, fired)`, after | 3,075 / 3,126 | 365 / 351 | 9,273,344 / 9,404,416 |

A fire update was not HOT before. The predicate of
`idx_harvest_timers_pending` names `fired`. So the update already added an
entry to each index, and the change adds no index writes. The index count is
the same after both migrations.

## Equivalence

The change adds no query and changes no query text.

* Each worker-driven run leaves the same timer digest before and after: an
  md5 over `timer_id` and `fired` of each timer, in `timer_id` order. Each run
  has `n` `TimerFired` events.
* [`equivalence.sql`](perf-artifacts/timer-pending-exec-lookup/equivalence.sql)
  runs each lookup for each run of the fixture, at one fixed `asof`. It
  hashes the full ordered result, with `id` as the last sort key. All five
  hashes match: the due open timers (3,091 rows), the same read with
  `fired = true` (111,900 rows), one open timer by id (1,000 rows), a list of
  lagged and missing ids (7,603 rows), and a missing run (0 rows).
* The rebalance timer copy and delete, and the `ON DELETE CASCADE` from an
  execution, use the new index. The plan gate
  `rebalance_reads_children_and_timers_by_execution` passes unchanged,
  because the index keeps its name.

## Migrations and locks

* `20261110194313_harvest_timers_exec_fired_index` builds
  `idx_harvest_timers_exec_fired`. A plain build takes `SHARE` on
  `harvest_timers`. On the 1.13-million-row fixture, it took 534 ms.
* `20261110194400_harvest_timers_exec_swap` drops the old index and renames
  the new one to `idx_harvest_timers_exec`. The drop takes
  `ACCESS EXCLUSIVE` on `harvest_timers` for a short time: 40 ms on the
  fixture. The rename takes `SHARE UPDATE EXCLUSIVE` on the index. The drop
  is in its own migration, so it cannot deadlock with the build (issue
  #2395).
* Both files set `lock_timeout = '5s'` and give an out-of-band
  `CONCURRENTLY` recipe in the header.
* The down migrations restore the old index. Up, down and up again ran
  cleanly on the fixture. A second up on the swapped state takes no lock.

## Reproduce

Load `pg_stat_statements` in `shared_preload_libraries`. Run the capture on
the baseline commit with `PERF_LABEL=baseline` and on the fix commit with
`PERF_LABEL=after`:

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=after cargo test -p autumn-harvest --features db,testing \
  --test integration -- --ignored --exact \
  timer_pending_exec_lookup_perf::zz_capture_timer_pending_exec_lookup_evidence \
  --nocapture
```

The static fixture runs on a database with each migration applied:

```sh
cd docs/perf-artifacts/timer-pending-exec-lookup
psql -d timers -f seed.sql
psql -d timers -f measure.sql
createdb -T timers timers_grown
psql -d timers_grown -v n=100000 -f grow.sql -f auto-mode.sql
psql -d timers -f write.sql
psql -d timers -v asof="$(date -u +%FT%TZ)" -f equivalence.sql
psql -d timers -f prefix-uses.sql
```

The files `baseline-*` and `after-*` hold the worker-driven captures. The
files `fixture-baseline-*` and `fixture-after-*` hold the static plans,
sizes and write costs.
