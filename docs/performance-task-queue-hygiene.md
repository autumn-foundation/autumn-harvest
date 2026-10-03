# Task-queue hygiene: claim latency after 1M terminal rows

Issue #1811. This page records claim latency with 1M terminal
`harvest_task_queue` rows, before and after the fix. The fix has two parts:

- the terminal-task janitor (`queue::sweep_terminal_tasks`);
- migration `20261003201739_harvest_task_queue_hygiene`, which tunes the
  table and reshapes its indexes.

## Result

| arm | live rows | dead rows | heap MB | index MB | n | p50 ms | p99 ms | max ms | claims/s |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline | 10 000 | 0 | 2.4 | 2.2 | 720 | 302.68 | 438.41 | 492.00 | 26 |
| before-dead | 1 010 000 | 999 866 | 349.8 | 186.5 | 720 | 595.72 | 824.80 | 883.69 | 13 |
| before-vacuumed | 1 010 000 | 0 | 349.8 | 185.8 | 720 | 356.48 | 515.64 | 543.93 | 22 |
| after-swept | 10 000 | 0 | 2.4 | 241.7 | 720 | 304.16 | 416.07 | 467.94 | 26 |

The janitor deleted the 1M rows in 21 passes of up to 50 batches of 1000
rows, in 12.2 s.

What the numbers show:

- **Dead tuples are the main cost.** Before vacuum, 1M terminal rows double
  claim p50 (303 ms to 596 ms). Each row left a dead entry in the PENDING
  claim index when it moved to `COMPLETED`. The claim scan walks past those
  entries.
- **Vacuumed rows still cost.** With 1M live terminal rows, p50 is 18% above
  the baseline (356 ms). The heap is 146 times larger, so fewer hot pages
  fit in cache.
- **After the sweep, claims match the baseline.** p50 is 304 ms against
  303 ms, and the heap goes back to 2.4 MB.
- **Index files do not shrink.** VACUUM makes the freed index pages
  reusable, but the files keep their size (241.7 MB). New entries reuse the
  pages. To return the space after the first sweep of a large backlog, run
  `REINDEX INDEX CONCURRENTLY` on each `harvest_task_queue` index once.

In steady state the janitor runs every tick, so the table holds at most
about 7 days of terminal rows. The autovacuum settings start a vacuum at 2%
dead rows, not 20%, so the dead-tuple debt of the `before-dead` arm is
cleared sooner.

## Plans

The migration replaces `idx_harvest_tq_running`, keyed on
`last_heartbeat_at`, with `idx_harvest_tq_running_started`, keyed on
`started_at`. Both have `WHERE state = 'RUNNING'`. The claim plan is the same
under both schemas. The two timeout scans read the new index where they read
the old one:

```text
before  heartbeat_timeout:      Bitmap Index Scan on idx_harvest_tq_running
after   heartbeat_timeout:      Bitmap Index Scan on idx_harvest_tq_running_started
before  start_to_close_timeout: Bitmap Index Scan on idx_harvest_tq_running
after   start_to_close_timeout: Bitmap Index Scan on idx_harvest_tq_running_started
```

The full output, with every claim-plan scan node, is in
`docs/perf-artifacts/task-queue-hygiene/bench-1m.txt`.

A heartbeat changes only `last_heartbeat_at` and `heartbeat_details`. No
index now uses either column, so a heartbeat update can be HOT. The test
`a_heartbeat_update_is_hot` in
`autumn-harvest/tests/integration/terminal_task_gc_tests.rs` proves this.
On the old schema the same update is not HOT.

## Method

Each arm truncates the queue and seeds the headline claim scenario: 10 000
pending rows, 8 claimers, 4 queues. The arms differ in schema and terminal
rows:

| arm | schema | terminal rows |
|---|---|---|
| `baseline` | after | none |
| `before-dead` | before | 1M, not vacuumed |
| `before-vacuumed` | before | 1M, vacuumed |
| `after-swept` | after | 1M, deleted by the janitor, then vacuumed |

- "Before" applies the migration's `down.sql`. "After" applies its `up.sql`.
- Each terminal row enters as `PENDING` in a bench queue and then moves to
  `COMPLETED` 30 days ago, as a real row does.
- Autovacuum is off on the table during the run, so each arm's vacuum
  state is exact.
- Claims run through the shared `claim_bench` harness
  (`db::measure_seeded_claims`), so the numbers compare with
  [`performance.md`](performance.md).

Reproduce:

```text
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo bench -p autumn-harvest --features db --bench task_queue_hygiene_bench
```

`HARVEST_BENCH_TERMINAL_ROWS` sets the terminal row count (default
1 000 000).

## Reference environment

| | |
|---|---|
| Machine | Linux, 4 logical CPUs (cloud container) |
| Postgres | 16.14, local, stock config |
| Profile | `bench` (release) |
| Harness | `autumn-harvest/benches/task_queue_hygiene_bench.rs` |

## Limits

Each arm is one sample on one host. The numbers show the shape of the cost,
not an SLO. The concurrent load is the claimers only. No heartbeat or
completion traffic runs during the measurement.
