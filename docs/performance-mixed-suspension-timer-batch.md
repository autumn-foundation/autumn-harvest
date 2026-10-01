# The mixed-suspension per-timer N+1

A workflow that arms `N` durable timers in one suspension, for example
`ctx.race()` over `N` `.timer(..)` branches, reaches
`persist_mixed_suspension_batch` (`autumn-harvest/src/worker.rs`). That path,
and the history-cap preflight `suspended_command_event_count` in front of it,
resolved each `StartTimer` with its own `SELECT ... FROM harvest_timers WHERE
workflow_exec_id = $1 AND timer_id = $2 AND NOT fired LIMIT 1`. The persist
path then read `SELECT NOW()` once per new timer and issued one single-row
`INSERT INTO harvest_timers` per new timer.

> **This is a reference measurement, not an SLO.** One machine, one Postgres
> configuration. Reproduce it before designing against it.

## TL;DR

* **Statements per park go from `4n` to 4.** At n=10: 40 → 4. The count no
  longer depends on `n`.
* **Buffers for those statements fall 85.7% at n=10** (4,811 → 689, tool:
  `pg_stat_statements`). Rows filtered by the lookup scans fall 90%
  (20 × 1,010 → 2 × 1,010).
* **No migration, no new index, no schema change.**
* The per-timer INSERT and `NOW()` reads are batched in the same pass. They are
  the same loop. The sweep below reports lookup, clock and insert separately.

## Reference environment

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=after cargo test -p autumn-harvest --features db,testing \
  --test integration -- --ignored --exact \
  mixed_suspension_timer_batch_perf::zz_capture_mixed_suspension_timer_batch_evidence \
  --nocapture
```

| | |
|:--|:--|
| Postgres | 16.14, `pg_stat_statements` preloaded, default `shared_buffers` |
| Harness | `autumn-harvest/tests/integration/mixed_suspension_timer_batch_perf.rs` |
| Fixture | 20,000 executions, about 110,000 `harvest_timers` rows, 5% still pending, timers per execution skewed 1 to 10 |
| Artifacts | `docs/perf-artifacts/mixed-suspension-timer-batch/` (`baseline-*`, `after-*`) |

The harness runs a real `Worker` against a fresh, migrated database. It resets
`pg_stat_statements` after seeding and reads it after the workflow parks.

## Profile

At n=10 the target statements are 40 of 147 statements (27%) and 4,811 of
11,572 buffers (42%) the worker issued during the park. The denominator
includes background scanner statements, so the shares are a lower bound.

## Plan

`harvest_timers` has no `(workflow_exec_id, timer_id)` index. The planner
filter-scans `idx_harvest_timers_pending`, which holds every un-fired timer in
the table (1,010 rows in the fixture, 230 buffers per call, see
`baseline-explain.txt`). The batched lookup uses the same scan, once.

## Hypothesis

Each per-timer lookup pays a full pending-index scan, so cost is `n` scans
instead of one. `eq_any` turns `n` scans into one.

## Change

`load_unfired_timers_by_id` loads every un-fired row for the batch's timer ids
in one statement. Both loops use it. The persist loop reads `NOW()` at most
once, because `NOW()` is the transaction start time and returns the same value
on every call. The new timer rows go in one multi-row `INSERT`. Both the lookup and the insert split into chunks under PostgreSQL's 65,535 bind-parameter limit (21,845 rows per insert chunk, 65,533 ids per lookup chunk), so a very wide batch still parks. A timer id is
unique within a batch (`duplicate_start_timer_id` rejects repeats), so the
batched insert writes the same rows. No migration, no lock.

## Measurement

Tool: `pg_stat_statements`, same fixture, same session.

| n | lookup calls | lookup buffers | `NOW()` calls | INSERT calls | target calls | target buffers |
|--:|--:|--:|--:|--:|--:|--:|
| 3 before | 6 | 1,374 | 3 | 3 | 12 | 1,472 |
| 3 after | 2 | 458 | 1 | 1 | 4 | 556 |
| 10 before | 20 | 4,580 | 10 | 10 | 40 | 4,811 |
| 10 after | 2 | 458 | 1 | 1 | 4 | 689 |
| 40 before | 80 | 18,320 | 40 | 40 | 160 | 18,851 |
| 40 after | 2 | 458 | 1 | 1 | 4 | 989 |

The two remaining lookups are the preflight and the persist path. Temp blocks
are zero. The INSERT buffers still grow with `n`, because the rows themselves are new.
WAL for the timer INSERT is 2,480 bytes at n=10 before (see the snapshot);
the multi-row form writes the same rows.

## Equivalence

* All 16 `mixed_suspension_tests` pass unchanged (run with `--test-threads=1`
  against one shared database). They cover re-park of a still-pending timer,
  cancel plus start, and races resolved by each branch.
* `park_persists_the_same_timer_rows_and_events` checks row count, distinct
  ids, one `TimerStarted` per timer, and `fires_at` spacing.
* `harvest_timers` has no unique index, so two un-fired rows for one
  `timer_id` are possible in principle. The old `LIMIT 1` took an arbitrary
  row. The batched lookup takes the earliest `fires_at`, then the lowest `id`.
  The tests do not cover this case.
* Isolation and transaction boundaries are unchanged.

## Not done

* An index on `(workflow_exec_id, timer_id) WHERE NOT fired` would cut the
  remaining 2 scans. It adds write cost on every timer insert and a migration.
  It needs a human decision and its own write-cost measurement.
* The sibling per-timer loops in `ArmTimer` / `CancelTimer` persist and
  `persist_race_loser_cancellations` have the same shape and were not touched.

## Reproduce

Run the command above with `PERF_LABEL=baseline` on the baseline commit and
`PERF_LABEL=after` on the fix commit.
