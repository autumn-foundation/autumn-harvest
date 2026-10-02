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
./benchmarks/notify-commit.sh
```

The script starts a throwaway `postgres:16` container, runs
`autumn-harvest/benches/notify_commit_bench.rs`, and writes the report to
`benchmarks/results/`. `HARVEST_NOTIFY_BENCH_SECS` sets the window of each
scenario, and `HARVEST_NOTIFY_BENCH_WRITERS` sets the writer counts.

## Results

Measured on 2026-10-02 in a 4-vCPU Linux container (Intel Xeon, 2.1 GHz). The
benchmark and Postgres 16 shared the host, with default Postgres settings. The
window was 5 s per scenario.

| writers | mode | commits/s | p50 ms | p99 ms |
|---:|---|---:|---:|---:|
| 1 | none | 943 | 0.84 | 2.85 |
| 1 | in_transaction | 582 | 1.39 | 5.90 |
| 1 | post_commit | 581 | 1.65 | 3.47 |
| 4 | none | 2461 | 1.51 | 4.08 |
| 4 | in_transaction | 1699 | 2.22 | 5.50 |
| 4 | post_commit | 1579 | 2.37 | 6.37 |
| 16 | none | 4328 | 3.22 | 10.65 |
| 16 | in_transaction | 1030 | 8.03 | 32.20 |
| 16 | post_commit | 3001 | 4.84 | 14.69 |
| 32 | none | 5271 | 5.52 | 15.78 |
| 32 | in_transaction | 1714 | 17.81 | 35.38 |
| 32 | post_commit | 3162 | 9.19 | 26.71 |
| 64 | none | 4671 | 12.59 | 32.84 |
| 64 | in_transaction | 1227 | 47.75 | 157.92 |
| 64 | post_commit | 2990 | 19.04 | 58.46 |

## Reading the numbers

- From 16 writers up, `post_commit` commits 1.8 to 2.9 times as many
  transactions per second as `in_transaction`. At 64 writers its p99 is 58 ms,
  not 158 ms.
- With 1 or 4 writers the two modes are about equal. The lock is not
  contended there. The sender adds two short statements per tick, and the
  write connection still makes one round trip to stage the note.
- `post_commit` stays below `none`. The stage statement and the sender
  statements use the same database.
- These numbers are a guide for this host, not a promise. Run the script on
  your own hardware to size a deployment.
