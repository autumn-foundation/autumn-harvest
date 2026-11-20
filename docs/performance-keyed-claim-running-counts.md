# The keyed claim counted every running task

This page follows issue #2744. The window claim of
`queue::claim_task_one_statement` has the CTE `seek_running_counts`. It
counts the `RUNNING` rows of each pending pair of concurrency key and task
type. The candidate gate compares that count with the cap. The old count
read every `RUNNING` row of the shard and then joined the rows to the
pending keys. So a claim of a keyed task read about 1,090 buffers, and a
claim with no key read 97. `docs/design/DESIGN-2744.md` records the
decision.

## 🎯 Workload

Two harnesses on PG 16 with `pg_stat_statements` and the default
`shared_buffers`. Evidence is calls and buffers, not wall-clock.

1. **Harness.** `concurrency_live_index_perf`, from the branch of issue
   #2678 (PR #2746), unchanged. It runs 2,000 tasks through
   `queue::enqueue`, `queue::claim_task` and `queue::complete_claimed_task`.
   One phase gives each task one of 20 keys and a high cap. The other phase
   gives no key. The background holds 100,000 pending rows with no key, 2,000
   pending keyed rows and 1,000 `RUNNING` keyed rows. It runs once with
   `idx_harvest_tq_concurrency_live` and once without it.
2. **Plans.** `psql` runs the window claim text of `keyed-claim-before.sql`
   and `keyed-claim-after.sql` under the claim settings, on the fixture of
   `keyed-claim-fixture.sql`. The two texts differ in `seek_running_counts`
   only. Three growth steps then change the running rows. A fourth file plans
   the full-scan count over a backlog of many keys.

The artifacts are in `docs/perf-artifacts/keyed-claim-running-counts/`.

## 📈 Profile (before)

Claim buffers per call, from `harness-before/write-costs.md`:

| index | keyed claim | plain claim |
|:--|--:|--:|
| without | 1,094.3 | 97.3 |
| with | 1,096.3 | 97.3 |

The index of issue #2678 does not change the cost. The difference is the
count step.

## 🧭 Plan

Before (`keyed-claim-plan-before.txt`):

```
CTE seek_running_counts
  ->  HashAggregate  Buffers: shared hit=1001
        ->  Hash Semi Join  (Hash Cond: t.concurrency_key = seek_pending_keys.concurrency_key)
              ->  Index Scan using harvest_task_queue_running_worker_idx on harvest_task_queue t  (rows=1000)
```

After (`keyed-claim-plan-after.txt`):

```
CTE seek_running_counts
  ->  Nested Loop  Buffers: shared hit=22
        ->  HashAggregate  (Group Key: seek_pending_keys.concurrency_key, rows=20)
        ->  HashAggregate  (Group Key: t.task_type, loops=20)
              ->  Index Scan using harvest_task_queue_concurrency_key_running on harvest_task_queue t  (loops=20)
                    Index Cond: (concurrency_key = seek_pending_keys.concurrency_key)
```

## 🔧 Change

The window CTEs `seek_running_counts` and `seek_deep_running_counts` now
read `claim_window_running_counts_sql!`. It probes once per distinct
pending key:

```sql
SELECT k.concurrency_key, c.task_type, c.running_count
FROM (SELECT DISTINCT concurrency_key FROM seek_pending_keys) k
CROSS JOIN LATERAL (
    SELECT t.task_type, COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.concurrency_key = k.concurrency_key
      AND t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
    GROUP BY t.task_type
) c
```

- Postgres does not pull up a subquery with an aggregate or a `DISTINCT`.
  So the plan is one probe of `harvest_task_queue_concurrency_key_running`
  per key, whatever the estimates.
- The probe groups by task type. So a key with two task types costs one
  probe, and each pair has one row at most.
- A pair with no running row has no row, as before. The gate reads
  `COALESCE(count, 0)`, so the claim result does not change.
- The full scan keeps `concurrency_running_counts` in its one-pass form.
  Its keys are the due keys of the whole backlog. See
  [The full scan](#the-full-scan).
- No migration and no new index. The recheck in `claimed` and
  `release_claim_if_over_cap` do not change.

## 📊 Result

Claim buffers per call, from `harness-after/write-costs.md`:

| index | keyed claim before | keyed claim after | plain claim after |
|:--|--:|--:|--:|
| without | 1,094.3 | 112.2 | 97.2 |
| with | 1,096.3 | 114.4 | 97.3 |

The keyed claim reads about 90% fewer buffers. It now reads 15 to 17 more
than a plain claim, for 20 key probes. The buffers of the enqueue, the
completion and the plain claim do not change. The WAL bytes of each
statement stay within 2%.

The count step and the whole window claim, from the `psql` plans:

| running rows | before: count | after: count | before: claim | after: claim |
|:--|--:|--:|--:|--:|
| 1,000 of other keys (fixture) | 1,001 | 22 | 1,094 | 114 |
| 10,000 of other keys, appended | 276 | 42 | 372 | 138 |
| 10,000 of other keys, across the heap | 9,652 | 42 | 9,749 | 138 |
| plus 1,000 of its own 20 keys | 303 | 562 | 425 | 684 |

- **Other keys.** After the change, more running rows of other keys add no
  reads. The count grows from 22 to 42 because the key index gains a
  level, so each of the 20 probes reads one more page.
- **Across the heap.** `scattered-growth.sql` claims random background rows
  in place, as a live queue does. The old count then reads about one page
  per running row: 9,652 for 10,000. The appended rows of the second line
  sit in a few pages, so the old count read only 276 there.
- **Own keys.** The count reads the running rows of its own keys. With 50
  running rows on each of the 20 keys, it reads 562 buffers for 1,000 rows.
  Each probe reads the pages of its own rows, so a page with rows of
  several keys is read once per key. The old count read 303 here because the
  fixture appends all 11,000 running rows to a few pages. A claim of a key
  with many running tasks pays for those tasks. That is the cost that the
  issue accepts.

## The full scan

The full scan, an ageing claim and a fair claim read
`concurrency_running_counts`. Its pending keys are the due keys of the whole
backlog, not of a window. So they can be thousands. The one-pass form lets
the planner choose between one pass over the running rows and one probe per
key.

`backlog-keys.sql` adds 8,000 due rows, each with its own key, and plans
both forms (`backlog-keys-plan.txt`). The one-pass form chose one probe per
key on its own. Both forms read about 18,600 buffers, and 2,576 of them are
the scan of the pending keys. A forced probe per key gains nothing there.
On a small backlog the planner can pick one pass instead. So the full scan
keeps its form.

## ✅ Gates

`autumn-harvest/tests/integration/keyed_claim_running_counts_tests.rs` is in
the CI manifest. It plans the count CTEs with `CLAIM_PLAN_SETTINGS_SQL`.
`gate-readings.txt` records the local readings.

- The buffer gate bounds one window count at 50 buffers for 4 keys and 13
  own running rows, with 1,000 running rows of other keys. It also checks
  for one key probe per key. Before the change it read 978. After it
  reads 7.
- The growth gate adds 5,000 running rows of other keys and with no key. A
  window count may then read up to two pages more per probe. It reads 11:
  the key index gains one level.
- The result gate compares each count of all three CTEs with a direct count
  per pair. It covers a row with no worker, a row in another state and a key
  with two task types. It also covers a key with no running row and a
  running row with no cap.

The shape test `window_running_counts_probe_once_per_pending_key` in
`queue.rs` pins the per-key text of both window CTEs and the one-pass text
of the full scan, for every claim form.

## 🔁 Reproduce

```sh
psql -f docs/perf-artifacts/keyed-claim-running-counts/keyed-claim-fixture.sql
psql -f docs/perf-artifacts/keyed-claim-running-counts/keyed-claim-before.sql
psql -f docs/perf-artifacts/keyed-claim-running-counts/keyed-claim-after.sql
```

Run the growth files between plan runs: `foreign-growth.sql`, then
`own-growth.sql`. Run `scattered-growth.sql`, then `backlog-keys.sql`, on a
fresh fixture. The harness runs with `cargo test -p autumn-harvest --test
integration -- concurrency_live_index_perf --ignored` on the branch of PR
#2746.
