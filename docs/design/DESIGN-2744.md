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

Design-time `psql` runs on PG 16, with no committed artifact. The seed has
the background of `concurrency_live_index_perf`: 100,000 pending rows,
2,000 pending keyed rows and 1,000 `RUNNING` keyed rows. Queue `w` adds 200
pending rows and 20 running rows on 20 keys. Each run applies the claim
settings and a generic plan. The committed plans of the Ledger page read
1,001 and 22 on the fixture of the harness.

| form | plan of the count | count buffers |
|:--|:--|--:|
| today: `IN` and `GROUP BY` | index scan of `harvest_task_queue_running_worker_idx`, hash semi join | 1,002 |
| `LATERAL` count per pending pair | nested loop, index scan of `harvest_task_queue_concurrency_key_running` per pair | 40 |

- `harvest_task_queue_concurrency_key_running` is on `(concurrency_key)`
  where `state = 'RUNNING' AND concurrency_key IS NOT NULL`. An equality on
  the key proves that predicate.
- Postgres does not pull up a subquery with an aggregate or a `DISTINCT`.
  So a `LATERAL` count stays a nested loop with one probe per key, whatever
  the estimate.
- The gate reads `COALESCE(lookup, 0)`. A pair with no running row has no
  row today, and the gate reads 0.
- Each candidate row of a scan has its pair in the pending keys of that scan.
  So a count for a pair that is not a pending pair is never read.
- A window holds at most a few hundred rows, so its pending keys are few. The
  full scan reads the due keys of the whole backlog, so its keys can be
  thousands.

## Brainstorm (green hat)

1. A `LATERAL` count per pending pair over the existing key index.
2. A scalar subquery per pending pair in the select list.
3. A new index on `(concurrency_key, task_type) WHERE state = 'RUNNING' AND
   worker_id IS NOT NULL`, with the query of today.
4. `AND t.concurrency_key IS NOT NULL` in the query of today.
5. A counter table of running tasks per pair.
6. Count in the candidate gate per row, with no CTE.
7. Use `idx_harvest_tq_concurrency_live` from the gauge change.
8. A `LATERAL` count per distinct pending key, grouped by task type.
9. Option 1 or 8 for the window CTEs only. The full scan keeps one pass.

## Reverse brainstorm: how to make it worse

| way to fail | guard in this design |
|:--|:--|
| Count running rows with no worker | The probe keeps `t.worker_id IS NOT NULL`. A DB test seeds such rows. |
| Merge the counts of two task types | The probe groups by task type. A DB test seeds a key with two types. |
| Return two rows for one pair, so the gate lookup fails | The probe takes each key once with `DISTINCT` and groups by task type. |
| Read a key twice when it is pending under two task types | One probe per key, not per pair. |
| Emit a zero row for each pending pair, so the per-row gate lookup scans a long CTE | `GROUP BY` emits only pairs with running rows, as before. |
| Probe once per key over a backlog of thousands of due keys | The full scan keeps one pass. The planner picks a probe per key only when it costs less. |
| Let the planner go back to a scan of every running row | A buffer gate seeds many foreign running rows and bounds the reads. A plan check counts the probes. |
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
  count: a running row with a key and no cap counts today. Option 1 reads a
  key twice when it is pending under two task types. It also emits a zero
  row for each pair. On the full scan it probes once per due key of the
  backlog.
- **Yellow.** Option 8 reads 1 to 2 buffers per pending key and one heap
  page for each running row of that key. A keyed claim no longer pays for
  the running rows of other keys. It needs no migration and no new write.
- **Green.** Options 1 to 9.
- **Blue.** Build options 8 and 9: a new macro for the two window CTEs. Prove
  it with a buffer gate and a result test first. Update the shape tests.
  Measure with `concurrency_live_index_perf` and record the result on a
  Ledger page.

## Decision

`claim_window_running_counts_sql!` builds `seek_running_counts` and
`seek_deep_running_counts`. It counts per distinct pending key:

```sql
SELECT k.concurrency_key, c.task_type, c.running_count
FROM (SELECT DISTINCT concurrency_key FROM <keys>) k
CROSS JOIN LATERAL (
    SELECT t.task_type, COUNT(*) AS running_count
    FROM harvest_task_queue t
    WHERE t.concurrency_key = k.concurrency_key
      AND t.state = 'RUNNING'
      AND t.worker_id IS NOT NULL
    GROUP BY t.task_type
) c
```

`concurrency_running_counts` keeps `claim_running_counts_sql!`, the one-pass
form. No migration. No new index. The recheck in `claimed` and
`release_claim_if_over_cap` do not change.

The first draft used option 1 for all three CTEs. Review found the zero
rows, the double read and the cost on a backlog of many due keys. So the
design moved to options 8 and 9.

## Acceptance criteria

1. A keyed claim reads buffers in proportion to the running rows of its own
   keys, not to every running row.
2. The claim results do not change. The claim shape tests pass.
3. The change is measured with `concurrency_live_index_perf` and recorded on
   a Ledger page.

## Test plan

- RED: `keyed_claim_running_counts_tests` plans each window count CTE under
  the claim settings. The fixture has many running rows of other keys. The
  gate bounds the buffers and counts the key probes. A growth gate checks
  that more foreign running rows add no reads. A result test compares each
  count of all three CTEs with a direct count per pair.
- RED: the shape test pins the per-key text of the window CTEs and the
  one-pass text of the full scan, in every claim form.
- GREEN: the macro change. The claim shape tests, `claim_concurrency_tests`,
  `claim_seek_tests` and the window tests pass.
- Evidence: `concurrency_live_index_perf`, before and after, on the branch of
  the gauge change. The Ledger page is
  `docs/performance-keyed-claim-running-counts.md`.
