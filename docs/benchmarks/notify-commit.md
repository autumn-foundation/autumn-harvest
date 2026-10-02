# Commit throughput with NOTIFY (issue #1796)

A transaction that calls `pg_notify` takes a database-wide lock at commit.
Concurrent commits that notify then run one at a time. Issue #1796 moved the
NOTIFY for history appends and enqueues out of the write transaction. A sender
for each pool sends it after commit, on its own connection.

This page records commit throughput for N concurrent writers in three modes:

| Mode | What one write transaction does |
|---|---|
| `none` | One `INSERT`. This is the upper bound. |
| `in_transaction` | One `INSERT` and one `pg_notify`. Harvest did this before issue #1796. |
| `post_commit` | One `INSERT` and `notify::notify_task_enqueued`, with the pool registered. |

## Run it

```sh
HARVEST_NOTIFY_BENCH_WRITERS=1,4,16,32,64 ./benchmarks/notify-commit.sh
```

The script starts a throwaway `postgres:16` container, runs
`autumn-harvest/benches/notify_commit_bench.rs`, and writes the report to
`benchmarks/results/`. `HARVEST_NOTIFY_BENCH_SECS` sets the window of each
scenario. `HARVEST_NOTIFY_BENCH_WRITERS` sets the writer counts, and the
default is `1,4,16,32`.

Each scenario starts from an empty table. The order of the modes rotates for
each writer count, so no mode always runs last. A listener drains the queue
channel and counts the wakes it receives. `lost` is the failure count of the
sender in that scenario.

## Results

Measured on 2026-10-02 in a 4-vCPU Linux container (Intel Xeon, 2.1 GHz). The
benchmark and Postgres 16 shared the host, with default Postgres settings. The
window was 5 s per scenario.

| writers | mode | commits/s | p50 ms | p99 ms | wakes | lost |
|---:|---|---:|---:|---:|---:|---:|
| 1 | none | 802 | 1.18 | 2.79 | 0 | 0 |
| 1 | in_transaction | 493 | 1.84 | 4.77 | 2466 | 0 |
| 1 | post_commit | 559 | 1.70 | 3.64 | 2504 | 0 |
| 4 | in_transaction | 1844 | 2.06 | 4.59 | 9218 | 0 |
| 4 | post_commit | 1726 | 2.19 | 5.43 | 2694 | 0 |
| 4 | none | 2692 | 1.40 | 3.38 | 0 | 0 |
| 16 | post_commit | 3320 | 4.32 | 14.32 | 1325 | 0 |
| 16 | none | 5276 | 2.83 | 6.83 | 0 | 0 |
| 16 | in_transaction | 2326 | 6.75 | 11.74 | 11632 | 0 |
| 32 | none | 5973 | 4.88 | 13.27 | 0 | 0 |
| 32 | in_transaction | 2111 | 14.85 | 25.83 | 10555 | 0 |
| 32 | post_commit | 4235 | 7.08 | 17.32 | 908 | 0 |
| 64 | in_transaction | 1873 | 33.44 | 56.81 | 9365 | 0 |
| 64 | post_commit | 4398 | 13.41 | 36.48 | 576 | 0 |
| 64 | none | 6662 | 8.61 | 26.71 | 0 | 0 |

## Reading the numbers

- From 16 writers up, `post_commit` commits 1.4 to 2.3 times as many
  transactions per second as `in_transaction`. At 64 writers its p99 is
  36 ms, not 57 ms.
- With 1 writer, `post_commit` is a little faster. With 4 writers it is about
  6% slower, because the lock is not contended there. The write connection
  still makes one round trip to stage the note.
- `post_commit` sends far fewer wakes, because one tick merges the wakes of
  one queue. At 64 writers, 576 wakes covered about 22,000 commits.
- No notification was lost in any scenario.
- `post_commit` stays below `none`. The stage statement and the sender
  statements use the same database.
- Commit-to-wake latency is not in this table. The integration test
  `wake_latency_stays_within_tolerance_of_an_in_transaction_notify` bounds it.
  The sender reads an open transaction again at most 25 ms later.
- These numbers are a guide for this host, not a promise. Run the script on
  your own hardware to size a deployment.
