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
   only. Three growth steps then change the running rows.

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
  ->  Nested Loop  (rows=20)  Buffers: shared hit=22
        ->  CTE Scan on seek_pending_keys k  (rows=20)
        ->  Aggregate  (loops=20)
              ->  Index Scan using harvest_task_queue_concurrency_key_running on harvest_task_queue t  (loops=20)
                    Index Cond: (concurrency_key = k.concurrency_key)
```

## 🔧 Change

`claim_running_counts_sql!` counts per pending pair with a `LATERAL`
subquery:

```sql
SELECT k.concurrency_key, k.task_type, c.running_count
FROM seek_pending_keys k
CROSS JOIN LATERAL (
    SELECT COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.concurrency_key = k.concurrency_key
      AND t.task_type = k.task_type
      AND t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
) c
```

- Postgres does not pull up a subquery with an aggregate. So the plan is
  one probe of `harvest_task_queue_concurrency_key_running` per pair,
  whatever the estimates.
- The same macro builds `seek_deep_running_counts` and
  `concurrency_running_counts`. All three change.
- No migration and no new index. The recheck in `claimed` and
  `release_claim_if_over_cap` do not change.
- A pair with no running row gets a row with 0. The gate reads
  `COALESCE(count, 0)`, so the claim result does not change.

## 📊 Result

Claim buffers per call, from `harness-after/write-costs.md`:

| index | keyed claim before | keyed claim after | plain claim |
|:--|--:|--:|--:|
| without | 1,094.3 | 112.3 | 97.3 |
| with | 1,096.3 | 114.3 | 97.3 |

The keyed claim reads 89.7% fewer buffers. It now reads 15 more than a
plain claim, for 20 key probes. The enqueue, the completion and the plain
claim do not change. The WAL of each statement stays within 1%.

The count step and the whole window claim, from the `psql` plans:

| running rows | before: count | after: count | before: claim | after: claim |
|:--|--:|--:|--:|--:|
| 1,000 of other keys (fixture) | 1,001 | 22 | 1,094 | 114 |
| 10,000 of other keys, appended | 276 | 42 | 372 | 138 |
| 10,000 of other keys, across the heap | 9,677 | 42 | 9,774 | 138 |
| plus 1,000 of its own 20 keys | 303 | 562 | 425 | 684 |

- **Other keys.** After the change, more running rows of other keys add no
  reads. The count grows from 22 to 42 because the key index gains a
  level, so each of the 20 probes reads one more page.
- **Across the heap.** `scattered-growth.sql` claims random background rows
  in place, as a live queue does. The old count then reads about one page
  per running row, 9,677 for 10,000. The appended rows of the second line
  sit in a few pages, so the old count read only 276 there.
- **Own keys.** The count reads the running rows of its own pairs. With 50
  running rows on each of the 20 keys, it reads 562 buffers for 1,000 rows.
  Each probe reads the pages of its own rows, so a page with rows of
  several keys is read once per key. The old count read 303 here because the
  fixture appends all 11,000 running rows to a few pages. A claim of a key
  with many running tasks pays for those tasks. That is the cost that the
  issue accepts.

## ✅ Gates

`autumn-harvest/tests/integration/keyed_claim_running_counts_tests.rs` is in
the CI manifest. It plans each of the three count CTEs with
`CLAIM_PLAN_SETTINGS_SQL`.

- The buffer gate bounds one count at 50 buffers for 5 pairs and 14 own
  running rows, with 1,000 running rows of other keys. Before the change it
  read 978. After it reads 9.
- The growth gate adds 5,000 running rows of other keys and with no key. A
  count may then read one page more per probe, and no more. It reads 14:
  the key index gains a level.
- The result gate compares each count with a direct count per pair. It
  covers a row with no worker, a row in another state, a key with two task
  types, a key with no running row and a running row with no cap.

The shape test `every_running_count_probes_once_per_distinct_pending_pair`
in `queue.rs` pins the per-pair text and `SELECT DISTINCT` in each
pending-keys CTE, for every claim form.

## 🔁 Reproduce

```sh
psql -f docs/perf-artifacts/keyed-claim-running-counts/keyed-claim-fixture.sql
psql -f docs/perf-artifacts/keyed-claim-running-counts/keyed-claim-before.sql
psql -f docs/perf-artifacts/keyed-claim-running-counts/keyed-claim-after.sql
```

Run the growth files between plan runs: `foreign-growth.sql`, then
`own-growth.sql`. Run `scattered-growth.sql` on a fresh fixture. The harness
runs with `cargo test -p autumn-harvest --test integration --
concurrency_live_index_perf --ignored` on the branch of PR #2746.
