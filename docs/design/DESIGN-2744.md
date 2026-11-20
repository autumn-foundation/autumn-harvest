# DESIGN-2744: a per-key running count for the keyed claim

## Problem

A claim of a task with a concurrency key reads about 1,090 shared buffers. A
claim of a task with no key reads 97. The source is the window CTE
`seek_running_counts`, which `claim_running_counts_sql!` builds:

```sql
SELECT t.concurrency_key, t.task_type, COUNT(*) AS running_count
FROM harvest_task_queue t
WHERE t.state = 'RUNNING'
  AND t.worker_id IS NOT NULL
  AND t.concurrency_key IN (SELECT concurrency_key FROM seek_pending_keys)
GROUP BY t.concurrency_key, t.task_type
```

Under `CLAIM_PLAN_SETTINGS_SQL` the plan reads each `RUNNING` row through
`harvest_task_queue_running_worker_idx`. It then joins the rows to the pending
keys. So the cost follows every running task of the shard, keyed or not. The
same macro builds `concurrency_running_counts` and `seek_deep_running_counts`.

## Facts (white hat)

Design-time `psql` runs on PG 16, with the background of
`concurrency_live_index_perf`: 100,000 pending rows, 2,000 pending keyed rows
and 1,000 `RUNNING` keyed rows. Queue `w` adds 20 keys with 1 running row
each. Each run applies the claim settings and a generic plan.

| form | plan of the count | count buffers |
|:--|:--|--:|
| today: `IN` and `GROUP BY` | index scan of `harvest_task_queue_running_worker_idx`, hash semi join | 1,002 |
| `LATERAL` count per pending pair | nested loop, index scan of `harvest_task_queue_concurrency_key_running` per pair | 40 |

- `harvest_task_queue_concurrency_key_running` is on `(concurrency_key)`
  where `state = 'RUNNING' AND concurrency_key IS NOT NULL`. An equality on
  the key proves that predicate.
- Postgres does not pull up a subquery with an aggregate. So a `LATERAL`
  count stays a nested loop with one probe per pair, whatever the estimate.
- Each pending-keys CTE is `SELECT DISTINCT concurrency_key, task_type`. So
  each pair has one row, and the gate lookup returns one row at most.
- The gate reads `COALESCE(lookup, 0)`. Today a pair with no running row has
  no row. With a `LATERAL` count it has a row with 0. The gate result is the
  same.
- Each candidate row of a scan has its pair in the pending keys of that scan.
  So a count for a pair that is not a pending pair is never read.

## Brainstorm (green hat)

1. A `LATERAL` count per pending pair over the existing key index.
2. A scalar subquery per pending pair in the select list.
3. A new index on `(concurrency_key, task_type) WHERE state = 'RUNNING' AND
   worker_id IS NOT NULL`, with the query of today.
4. `AND t.concurrency_key IS NOT NULL` in the query of today.
5. A counter table of running tasks per pair.
6. Count in the candidate gate per row, with no CTE.
7. Use `idx_harvest_tq_concurrency_live` from the gauge change.

## Reverse brainstorm: how to make it worse

| way to fail | guard in this design |
|:--|:--|
| Count running rows with no worker | The probe keeps `t.worker_id IS NOT NULL`. A DB test seeds such rows. |
| Merge the counts of two task types | The probe matches the task type of the pair. A DB test seeds a key with two types. |
| Return two rows for one pair, so the gate lookup fails | A shape test pins `SELECT DISTINCT` in each pending-keys CTE. |
| Let the planner go back to a scan of every running row | A buffer gate seeds many foreign running rows and bounds the reads. A plan check names the key index. |
| Read the count once per candidate row | The CTE stays `MATERIALIZED`. A shape test pins it. |
| Depend on an index that is not on `trunk-dev` | The probe has no cap test, so only the existing key index matches. |
| Change the authoritative recheck in `claimed` | The recheck text does not change. Its shape test stays. |
| Break the window split, which drops CTEs by name | The CTE names do not change. The window tests run. |
| Claim a gain with no number | The harness of the gauge change measures the claim before and after. |

## Six hats

- **White.** The facts above.
- **Red.** Option 1 is what the issue proposes. It is short and reads well.
- **Black.** Option 3 adds an index and write cost to every claim and
  completion. Option 4 does not change the plan, as the issue records.
  Option 5 adds a write to every task. Option 6 is the per-row correlated
  count that the CTE replaced. Option 7 needs a cap test, which changes the
  count: a running row with a key and no cap counts today. A probe costs
  about 2 buffers for each pending pair, so 20 keys cost about 40.
- **Yellow.** Option 1 reads about 2 buffers per pending pair and one heap
  page for each running row of that pair. A keyed claim no longer pays for
  the running rows of other keys. It needs no migration and no new write.
- **Green.** Options 1 to 7.
- **Blue.** Build option 1 in the one macro, so all three CTEs change.
  Prove it with a buffer gate and a result test first. Update the shape
  tests. Measure with `concurrency_live_index_perf` and record the result on
  a Ledger page.

## Decision

`claim_running_counts_sql!` counts per pending pair:

```sql
SELECT k.concurrency_key, k.task_type, c.running_count
FROM <keys> k
CROSS JOIN LATERAL (
    SELECT COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.concurrency_key = k.concurrency_key
      AND t.task_type = k.task_type
      AND t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
) c
```

No migration. No new index. The recheck in `claimed` and
`release_claim_if_over_cap` do not change.

## Acceptance criteria

1. A keyed claim reads buffers in proportion to the running rows of its own
   keys, not to every running row.
2. The claim results do not change. The claim shape tests pass.
3. The change is measured with `concurrency_live_index_perf` and recorded on
   a Ledger page.

## Test plan

- RED: `keyed_claim_running_counts_tests` plans each of the three count CTEs
  under the claim settings. The fixture has many running rows of other keys
  and with no key. The gate bounds the buffers, names the key index, and
  checks that more foreign running rows add no reads. A result test compares
  each count with a direct count per pair.
- RED: the shape test pins the per-pair form and `SELECT DISTINCT` in each
  pending-keys CTE.
- GREEN: the macro change. The claim shape tests, `claim_concurrency_tests`,
  `claim_seek_tests` and the window tests pass.
- Evidence: `concurrency_live_index_perf`, before and after, on the branch of
  the gauge change. The Ledger page is
  `docs/performance-keyed-claim-running-counts.md`.
