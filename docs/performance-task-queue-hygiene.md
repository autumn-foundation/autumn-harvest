# Task-queue hygiene: claim latency after 1M terminal rows

Issue #1811. This page records claim latency with 1M terminal
`harvest_task_queue` rows, before and after the fix. The fix has two parts:

- the terminal-task janitor (`queue::sweep_terminal_tasks`);
- migration `20261003201739_harvest_task_queue_hygiene`, which tunes the
  table and reshapes its indexes.

## Result

| arm | live rows | dead rows | heap MB | index MB | n | p50 ms | p99 ms | max ms | claims/s |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline | 10 000 | 0 | 2.4 | 2.2 | 720 | 284.02 | 364.24 | 482.27 | 28 |
| before-dead | 1 010 000 | 996 745 | 349.8 | 188.3 | 720 | 488.36 | 584.86 | 632.62 | 16 |
| before-vacuumed | 1 010 000 | 0 | 349.8 | 185.4 | 720 | 280.00 | 365.49 | 479.18 | 28 |
| after-swept-unvacuumed | 10 000 | 1 999 511 | 387.4 | 242.1 | 720 | 540.05 | 660.05 | 748.06 | 15 |
| after-swept | 10 000 | 0 | 2.4 | 240.6 | 720 | 275.99 | 344.06 | 422.54 | 28 |

The janitor deleted the 1M rows in 21 passes of up to 50 batches of 1000
rows, in 10.3 s.

What the numbers show:

- **Dead tuples are the cost.** Before vacuum, 1M terminal rows raise claim
  p50 by 72% (284 ms to 488 ms). Each row left a dead entry in the PENDING
  claim index when it moved to `COMPLETED`. The claim scan walks past those
  entries.
- **Live terminal rows cost little.** Once vacuumed, 1M live terminal rows
  give the baseline p50 (280 ms against 284 ms). They are not in the
  claim's partial indexes.
- **The janitor's deletes are dead tuples too.** Right after the sweep and
  before vacuum, p50 is 540 ms. The deleted rows join the dead entries from
  the churn. After VACUUM, p50 is back at the baseline (276 ms), and the
  heap goes back to 2.4 MB.
- **So vacuum does the recovery, and the janitor bounds the work.** The
  migration starts autovacuum at 2% dead rows, not 20%. In steady state the
  janitor deletes at most 50 batches per shard per tick, so each pass leaves
  a small, bounded amount of dead tuples for autovacuum. After the first
  sweep of a large backlog, run `VACUUM (ANALYZE) harvest_task_queue` or let
  autovacuum finish before you judge claim latency.
- **Index files do not shrink.** VACUUM makes the freed index pages
  reusable, but the files keep their size (240.6 MB). New entries reuse the
  pages. The after schema also has `idx_harvest_tq_terminal_completed_at`,
  which held all 1M rows before the sweep. To return the space after the
  first sweep of a large backlog, run `REINDEX INDEX CONCURRENTLY` on each
  `harvest_task_queue` index once.

## Plans

The janitor's `DELETE` reads its own index, in key order:

```text
janitor_delete: Index Scan using idx_harvest_tq_terminal_completed_at on harvest_task_queue t
janitor_delete: Index Scan using harvest_task_queue_pkey on harvest_task_queue d
```

The bench's `harvest_dead_letters` table is empty, so the planner scans it
whole. With rows in it, the dead-letter check can use
`idx_harvest_dl_workflow_exec_id`.

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

The full output, with every scan node, is in
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
| `after-swept-unvacuumed` | after | 1M, deleted by the janitor, not vacuumed |
| `after-swept` | after | 1M, deleted by the janitor, then vacuumed |

- "Before" applies the migration's `down.sql`. "After" applies its `up.sql`.
- Each terminal row enters as `PENDING` in a bench queue and then moves to
  `COMPLETED` 30 days ago, as a real row does.
- Autovacuum is off on the table during the run, so each arm's vacuum
  state is exact. This also means the bench does not measure the new
  autovacuum thresholds. It measures the states that autovacuum moves
  between.
- The dead-row column comes from `pg_stat_user_tables`. After `ANALYZE`
  alone, it is an estimate.
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

Each arm is one sample on one host, in a fixed order. Differences of a few
percent are noise: an earlier run of the same bench put `before-vacuumed`
18% above the baseline, and this run puts it 1% below. The large effects
held in every run: dead tuples raise p50 by 70% to 100%, and vacuum
restores it. The
concurrent load is the claimers only. No heartbeat or completion traffic
runs during the measurement.
