# `sessions::enforce_broken_sessions` — two `session_id` seeks with no index

`enforce_broken_sessions` runs on every worker's timeout tick. It selects the
`ACTIVE` sessions whose host may be dead. For each one it seeks member tasks
in `harvest_task_queue` by `session_id`, twice. Migration
`20261003201739_harvest_task_queue_hygiene` dropped
`harvest_task_queue_session_id_pending`. That drop was correct: the index
predicate was `state = 'PENDING'`, and neither seek could prove it. But it
left both seeks with no index at all.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration. Reproduce it before you design
> against it. The harness is in the repo.

## 🎯 Workload

A fleet outage leaves many sessions with dead hosts. The next timeout tick on
a surviving worker finds all of them at once. The harness is
`autumn-harvest/tests/integration/broken_session_scan_perf.rs`. It calls
`enforce_broken_sessions` directly, the entry point `enforce_timeouts_once`
calls, against Postgres 16 with `pg_stat_statements` preloaded.

The fixture is seeded and deterministic. It sweeps n = 100, 400 and 1,600
sessions over 50n unrelated queue rows (90% terminal, 7% pending, 3% running,
no `session_id`). Sessions are skewed: 45% have no host row, 15% a stale
heartbeat, 10% a draining host, 10% an expired lease, 5% an expired lease
with a RUNNING member (not broken), and 15% are healthy decoys. Every
seventh session has a terminal owning execution. Odd-numbered sessions have
two member tasks.

## 📈 Profile

At n = 1,600 (`pg_stat_statements`, one pass, before the fix):

| Statement | Calls | Buffers | Share of buffers |
|:--|--:|--:|--:|
| member-task load, `session_id` + `state` | 1,327 | 1,613,632 | 80.8% |
| running-member probe, `session_id` + `state = 'RUNNING'` | 240 | 217,546 | 10.9% |
| both seeks | 1,567 | 1,831,178 | 91.7% |
| whole pass | 35,556 | 1,997,106 | 100% |

The seeks are 4.4% of calls and 91.7% of buffers. Buffers per call grow with
queue depth, and calls grow with n, so the cost is quadratic.

## 🧭 Plan

Before, at n = 1,600:

- Member load: `Bitmap Heap Scan` on `idx_harvest_tq_live_created`. It
  removes about 10,400 rows by filter to return 1. 1,216 buffers.
- Running probe: `Index Scan` on `idx_harvest_tq_running_started`. It removes
  2,400 rows by filter to return 1. 840 buffers.

After: both are an `Index Scan` on `idx_harvest_tq_session_active`, 3 buffers
each. Full plans are in `docs/perf-artifacts/broken-session-scan/`.

## 💡 Hypothesis

No index leads with `session_id`, so the planner reaches the rows through an
index on `state` or creation time and filters `session_id` afterwards. Each
seek reads every live or running row to find one session's tasks.

## 🔧 Change

Migration `20261008170951_harvest_task_queue_session_active_index` adds a
partial index:

```sql
CREATE INDEX idx_harvest_tq_session_active
    ON harvest_task_queue (session_id)
    WHERE session_id IS NOT NULL AND state IN ('PENDING', 'RUNNING');
```

Both seeks filter `state` to PENDING or RUNNING, so the planner proves the
predicate for either one. No query changed.

**Lock.** The build takes `SHARE` on `harvest_task_queue`, which blocks writes
until it ends. It fails after a 5 s lock wait. It built in 21 ms on 84,400
queue rows. The migration follows the guarded-build pattern in
[`docs/upgrading/online-migrations.md`](upgrading/online-migrations.md): an
operator can build the index first with `CREATE INDEX CONCURRENTLY`, and the
migration accepts it if it is valid and matches. `down.sql` took 1 ms.

## 📊 Measurement

`pg_stat_statements`, one pass. Buffer totals move about 0.5% between
identical runs.

| n | Seek buffers before | after | Pass buffers before | after | Change |
|--:|--:|--:|--:|--:|--:|
| 100 | 7,515 | 274 | 22,872 | 15,646 | -31.6% |
| 400 | 115,644 | 1,399 | 155,982 | 41,731 | -73.2% |
| 1,600 | 1,831,178 | 5,299 | 1,997,106 | 170,771 | -91.4% |

Statement count (35,556 at n = 1,600) and `temp_blks_written` (0) do not
change. The seeks read about 3.4 buffers per call after the fix. Before, the member
load read about 1,216 and the probe about 906. `idx_scan` on the new index
was 1,563 at n = 1,600, against 1,567 seeks.

## ✅ Equivalence

The harness dumps every session row (state, broken reason), every task row
(state, error) and the event type sequence of every execution, sorted. The
dumps before and after are byte-identical at all three sizes
(`cmp before-state-nN.txt after-state-nN.txt`). The fixture covers: hosts
with no row, stale, draining and stopped hosts, expired leases with and
without a RUNNING member, terminal owning executions, healthy decoys, and
one- and two-member sessions. No query text changed, so as-of and
bi-temporal semantics are not involved. Existing tests are unchanged.

## 💸 Write cost

500 session-pinned and 500 plain enqueues, `pg_current_wal_lsn()` delta,
after a `CHECKPOINT`. n = 1,600 fixture:

| | Before | After | Change |
|:--|--:|--:|--:|
| pinned enqueue, warm | 350,728 | 399,456 | +48,728 B (~97 B/row) |
| pinned enqueue, cold (full-page images) | 510,304 | 562,488 | +52,184 B |
| plain enqueue, warm | 350,336 | 350,568 | none |

A plain enqueue has `session_id` NULL, so it never enters the index. The
index was 88 KB at n = 1,600. `session_id` never changes after enqueue, and
`state` is already in the predicate of other indexes on this table, so HOT
updates are unaffected. A row leaves the index when its task turns terminal.

## 🔬 Reproduce

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/broken_session_scan_perf_repro.sh
```

## What this does not fix

`enforce_broken_sessions` still makes two or three re-verify reads per
candidate, and one transaction per broken session. Those are 8.5% of calls
and a small share of buffers. Batching them would widen the window between
the candidate scan and the re-check, so it needs a human decision.
