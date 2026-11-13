# Task-stats prune of each worker heartbeat

Each heartbeat of each worker runs `workers::prune_worker_task_stats` on its
shard (issue #1815). The prune deletes a row of `harvest_worker_task_stats`
when the row is older than the longer of two windows: the one-hour retention,
and the `peer_stale_secs` of the row's own cohort. That second window comes
from the cohort key stored in the row. So the threshold differs per row, and
no index can serve the predicate.

Before this change, each prune was a sequential scan of the whole table. It
read every row, decompressed its cohort key, and parsed the key as JSON. The
table holds one row per live worker and one row per worker id of the last
hour. So each heartbeat cost grew with the fleet, and the fleet cost grew with
the square of the fleet.

The prune now also states the bound that each deleted row satisfies:
`updated_at < NOW() - retention`. The threshold of a row is never shorter
than the retention, so this predicate removes no row that the old query
deleted. `harvest_worker_task_stats_updated_at_idx` serves it. The prune now
reads only the rows past the retention.

> **This is a reference measurement, not an SLO.** It comes from one machine
> with one Postgres configuration. Reproduce it before you design against it.

## TL;DR

* **Prune buffers fall 97.5%, 98.3% and 98.2%** for live fleets of 500,
  2,000 and 8,000 workers. Before, the prune was 42.0%, 73.6% and 91.7% of
  all buffers of the measured workers. After, it is 1.8%, 4.5% and 17.0%.
  Tool: `pg_stat_statements`, 4 real workers, a 60 s window.
* **All buffers of the window fall 40%, 73% and 90%.** The measured workers
  touch 20,118, 20,641 and 24,005 buffers instead of 33,815, 76,169 and
  243,595.
* **Rows read per prune fall from 10,467 to 65** on the fleet of 8,000.
  Tool: `EXPLAIN (ANALYZE, BUFFERS)` on the final state.
* **The plan changes at all three sizes.** Seq Scan becomes Index Scan, or
  Bitmap Heap Scan for a generic plan, on
  `harvest_worker_task_stats_updated_at_idx`. Its `idx_scan` goes from 0 to
  48-52 for the window. The `seq_scan` count of the table goes from 54-56 to 5.
* **Results are identical.** The rows left after the first prune of each
  fleet have the same md5 before and after. So do the rows that 19 edge rows
  leave, with two fallbacks. Existing tests pass unchanged.
* **No index, no migration, no schema change.** WAL of the window is within
  0.5%.

## The prune is a heartbeat cost

A fleet of `W` workers with a 5 s heartbeat runs `W / 5` prunes per second on
each shard. Each prune scanned all `W + churn` rows. This is a `per_tick`
statement whose cost scales with the fleet, not with the backlog.

The calls ranking does not show it. The prune is 48 of about 22,000
statements in each window (0.2%). Most statements are the idle polls of the
four workers, which cost 2 buffers each. The buffers ranking shows it at the
top at every size.

Each cohort key is wide. A worker with 30 registered workflow types writes a
key of about 6.9 KB. Postgres compresses the key in line, so the table has no
TOAST blocks. The scan still decompresses and parses each key on every
heartbeat.

## Workload

`autumn-harvest/tests/integration/worker_task_stats_prune_perf.rs` drives the
capture.

1. Start four real workers through `Worker::run`, at the production cadence:
   a 5 s heartbeat and a 500 ms poll. Each worker registers 30 workflow types.
   Scanner election is off, as on every worker of a shard but one.
2. Read the cohort key that the first heartbeat of `measured-0` wrote.
3. Seed [`seed.sql`](perf-artifacts/worker-task-stats-prune/seed.sql) beside
   the workers. Each fleet key is the measured key with another `build_id`.
4. Reset `pg_stat_statements`, run a 60 s window, and sample
   `pg_stat_activity` once a second.
5. Stop the workers. Read `pg_stat_statements`, `pg_stat_user_tables`,
   `pg_statio_user_tables` and `pg_stat_user_indexes`.
6. Run the public prune once in a transaction that rolls back. A Diesel
   instrumentation hook records its SQL. Explain that statement in custom and
   generic plan modes.

The fixture for a live fleet of `F`:

| Rows | Share | Age | Cohort |
|---|---|---|---|
| live workers | `F` | 0-4 s | 8 builds, 1 in 8 shares the measured cohort, 2% slow |
| orphans of restarted pods | `0.30 F` | 1-59 min | as above |
| past the retention | `0.02 F` | 62-122 min | half in a slow cohort |
| dead tuples | about 1 per live row | | |

A slow cohort has `peer_stale_secs = 10800`, so its rows outlive the
retention. `updated_at` is indexed, so each heartbeat write is a non-HOT
update. Autovacuum is off on the two tables for the fixture only, so the
dead-tuple ratio does not move during the window.

## Profile

`pg_stat_statements`, 4 measured workers, 48 heartbeats per window. The full
captures are
[`baseline-capture.txt`](perf-artifacts/worker-task-stats-prune/baseline-capture.txt)
and
[`after-capture.txt`](perf-artifacts/worker-task-stats-prune/after-capture.txt).

| Live fleet | Stats rows | Prune buffers | Share of buffers | Share of calls | Buffers per prune |
|--:|--:|--:|--:|--:|--:|
| 500 | 664 | 14,210 | 42.0% | 0.2% | 296.0 |
| 2,000 | 2,644 | 56,036 | 73.6% | 0.2% | 1,167.4 |
| 8,000 | 10,564 | 223,339 | 91.7% | 0.2% | 4,652.9 |

At every size the prune is first by buffers. The next statement is a poll
statement of the workers (`WITH worker_info AS ...`), at 5,229 to 5,308
buffers for about 475 calls. No statement
wrote temp blocks. The wait-event samples show the worker backends in
`Client:ClientRead` almost always: the workers wait on their own timers, not
on locks.

## Plan

Baseline, fleet of 8,000, custom plan
([`baseline-explain.txt`](perf-artifacts/worker-task-stats-prune/baseline-explain.txt)):

```text
Delete on public.harvest_worker_task_stats s (actual rows=0 loops=1)
  Buffers: shared hit=4650
  ->  Seq Scan on public.harvest_worker_task_stats s (actual rows=2 loops=1)
        Filter: (s.updated_at < (now() - ((GREATEST('3600'::bigint, LEAST(GREATEST(COALESCE((((s.cohort)::jsonb ->> 'peer_stale_secs'::text))::bigint, '120'::bigint), ...
        Rows Removed by Filter: 10466
```

After, same state
([`after-explain.txt`](perf-artifacts/worker-task-stats-prune/after-explain.txt)):

```text
Delete on public.harvest_worker_task_stats s (actual rows=0 loops=1)
  Buffers: shared hit=67
  ->  Index Scan using harvest_worker_task_stats_updated_at_idx on public.harvest_worker_task_stats s (actual rows=1 loops=1)
        Index Cond: (s.updated_at < (now() - '01:00:00'::interval))
        Filter: (s.updated_at < (now() - ((GREATEST('3600'::bigint, ...
        Rows Removed by Filter: 64
```

The generic plan keeps the bound as a parameter and uses a Bitmap Heap Scan
on the same index. Diesel sends `sql_query` text as an unnamed statement, so
each call gets a custom plan. Both modes are in the explain files.

## Change

One predicate in `prune_worker_task_stats`:

```sql
DELETE FROM harvest_worker_task_stats s
WHERE s.updated_at < NOW() - ($1::bigint * INTERVAL '1 second')
  AND s.updated_at < NOW() - (GREATEST($1::bigint, <cohort window>) * INTERVAL '1 second')
```

`$1` is the retention, so `GREATEST($1, x) >= $1` for every row. A row that
satisfies the second predicate satisfies the first. The binds do not change.

## Measurement

| Counter | Fleet | Before | After | Change | Tool |
|---|--:|--:|--:|--:|---|
| prune buffers | 500 | 14,210 | 356 | -97.5% | `pg_stat_statements` |
| prune buffers | 2,000 | 56,036 | 931 | -98.3% | `pg_stat_statements` |
| prune buffers | 8,000 | 223,339 | 4,070 | -98.2% | `pg_stat_statements` |
| buffers per prune | 500 / 2,000 / 8,000 | 296 / 1,167 / 4,653 | 7.4 / 19.4 / 84.8 | | `pg_stat_statements` |
| all buffers of the window | 500 | 33,815 | 20,118 | -40.5% | `pg_stat_statements` |
| all buffers of the window | 2,000 | 76,169 | 20,641 | -72.9% | `pg_stat_statements` |
| all buffers of the window | 8,000 | 243,595 | 24,005 | -90.1% | `pg_stat_statements` |
| rows read per prune | 8,000 | 10,467 | 65 | -99.4% | `EXPLAIN ANALYZE` |
| `seq_tup_read` of the table | 8,000 | 555,480 | 52,628 | -90.5% | `pg_stat_user_tables` |
| heap blocks of the table | 8,000 | 293,689 | 74,307 | -74.7% | `pg_statio_user_tables` |
| `idx_scan` of the `updated_at` index | 8,000 | 0 | 48 | | `pg_stat_user_indexes` |
| temp blocks written | all | 0 | 0 | | `pg_stat_statements` |
| WAL bytes of the window | 500 / 2,000 / 8,000 | 148,320 / 149,823 / 153,625 | 148,348 / 149,617 / 152,926 | within 0.5% | `pg_stat_statements` |
| statements of the window | 500 / 2,000 / 8,000 | 22,175 / 22,360 / 22,378 | 22,387 / 22,396 / 22,433 | | `pg_stat_statements` |

The statement count of a window moves by about 1%. The workers poll on a
timer, so a window holds a few more or fewer polls. Each window ran 48
prunes before and after.

After the change, the table has 5 sequential scans per window at each size.
They are not prune scans. Two are the digest reads of the harness after the
first prune. The prune now scans the index on each call. The peer read,
`load_live_worker_task_stats`, uses the hash index on `cohort` and is not in
the top 10 by buffers.

The remaining prune cost still grows with the fleet, from 7.4 to 84.8 buffers
per prune. Those are the rows of slow cohorts that are past the retention
but inside their own window, and the dead index entries of rows that the
prune deleted. A slow cohort must keep those rows, so the prune must read
them. The cost is bounded by the rows past the retention, not by the table.

## Equivalence

* **The first prune of each fleet.** The capture records the md5 of the
  ordered ids of the fleet rows after the first prune of the window:

  | Fleet | Rows after | md5, before and after |
  |--:|--:|---|
  | 500 | 655 | `68defbbd08b5a74166c50b2e0d731c8a` |
  | 2,000 | 2,616 | `e1b9dd76039eb115a541c54207e1a1a5` |
  | 8,000 | 10,464 | `0a2f5c3d7de6f6b01957d3ef631a4a93` |

* **Edge rows.** [`equivalence.sql`](perf-artifacts/worker-task-stats-prune/equivalence.sql)
  seeds 19 rows 2 s on either side of each threshold. They cover a fast
  cohort, a slow cohort, a key without `peer_stale_secs`, a JSON `null`, a
  negative value (clamped to 0), a value over one year (clamped), and an empty
  object. The prune ran with fallback 120 and with fallback 7,200. The kept
  ids are identical before and after. The structural test
  `the_prune_keeps_each_row_for_its_own_window` asserts them, and passes on
  both commits.
* **Existing tests.** `worker_saturation_metrics_tests` passes unchanged,
  including `frozen_stats_leave_the_live_set_and_old_rows_are_pruned`.
* **Error path.** The old prune parsed every key, so one stored key that is
  not JSON failed every prune. The new prune parses only the keys of rows
  past the retention. The doc comment says so. The writer always stores a
  JSON key, and `load_live_worker_task_stats` still parses each key of its
  cohort.

## Write cost

No index, no migration. The prune deletes the same rows, so its WAL does not
change. WAL of the whole window is within 0.5% at every size.

## Reproduce

```sh
# A Postgres 16 with pg_stat_statements preloaded, as a superuser.
export HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres

PERF_LABEL=baseline cargo test -p autumn-harvest --features db,testing \
    --test integration -- --ignored --exact \
    worker_task_stats_prune_perf::zz_capture_worker_task_stats_prune_evidence

# The structural test needs a plain database only.
cargo test -p autumn-harvest --features db,testing --test integration \
    -- worker_task_stats_prune_perf
```

Check out the RED commit for the baseline and the next commit for the after
capture. Each capture takes about 3.5 minutes: three 60 s windows and the
seeds.
