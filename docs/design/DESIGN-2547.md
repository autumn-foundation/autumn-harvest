# DESIGN-2547 — Refine the claim window by TID

Issue #2547. It follows issues #1956, #1971 and #2420.

## 1. Problem

The claim is 45.1% of the shared buffers of the e2e bench, at 141 buffers
per call. The refine step reads 122 of the 146 buffers in a median claim
plan. It reads each window row again through `harvest_task_queue_pkey`.
Each probe walks the index from the root, then reads the heap page. That is
about 4.2 buffers per row. The claim takes one row of about 29.

## 2. Facts

- The head scans read each window row from the heap. The head gates read
  heap columns, so the scan cannot be index-only.
- The refine must lock the table row with `FOR UPDATE SKIP LOCKED`. So any
  refine reads the heap page of each row it locks.
- All the CTEs and the refine run in one statement and one snapshot.
  Neither page pruning nor `VACUUM` removes a tuple that the snapshot
  sees. So a `ctid` read in `seek_heads` names the same tuple in the
  refine.
- A TID scan reads the heap page only. It reads no index page.
- `harvest_task_queue` is not partitioned. A `ctid` is unique in it.
- Postgres plans `ctid = ANY(<array>)` as a TID scan. In PG 12,
  `IsTidEqualAnyClause` in `tidpath.c` accepts the `ARRAY()` init plan.
  Harvest supports Postgres 12 and later.
- A peer can commit an update to a window row after the claim snapshot and
  before the claim locks the row. Postgres then rechecks the newest version
  (EvalPlanQual). The new version has a new `ctid`. `TidRecheck` in
  `nodeTidscan.c` decides the result:
  - 13.23, 14.20, 15.15, 16.11, 17.7 and later test the TID list. So the
    TID refine skips the row. A run on PG 16.15 shows this.
  - PG 12 and the older minor releases return `true`. So the refine
    rechecks every other gate on the new version, as a primary-key refine
    does.

  Section 5 covers the effect.

## 3. Brainstorming

| Question | Options |
|---|---|
| How the refine finds a window row | primary key (today) · **`ctid` from the head scan** · join on the window CTE |
| How the refine avoids a heap fetch | `INCLUDE` columns on `idx_harvest_tq_claim_seek` (issue mechanism 2) · **no change: the lock needs the heap page** |
| How the refine reads fewer rows | stop at the first passing row in key order (mechanism 1) · smaller window (mechanism 3) · **read all window rows, at about 1 buffer each** |
| Which scans change | **the window, the deep window and `seek_held`** · the window only |
| How a test proves the cost | total claim buffers · **refine buffers per refined row, at depth 32 and 1,000** |

## 4. Reverse brainstorming

How can this change make the claim worse?

| Way to fail | Guard |
|---|---|
| The `ctid` names another row | One snapshot. A visible tuple cannot be pruned. No partitions. |
| The planner picks a sequential scan | A TID scan is cheaper than any index scan. The RED test asserts a `Tid Scan` node and a bounded row count at each depth. |
| A concurrent update hides a row | The row is skipped, as `SKIP LOCKED` skips a held row. See section 5. |
| A gate or the order changes | Only the row source changes. Every gate and the sort stay in `claim_candidate_scan_sql!`. The order tests and the random drains run again. |
| A splice anchor breaks | The kind, fence, owner and fair splices anchor on other text. Their shape tests run again. |
| The `claimed` update or the lock set changes | `claimed` still joins on `id`. The refine locks the same rows. |
| Postgres 12 lacks the plan | PG 12 `tidpath.c` accepts `ctid = ANY` with a pseudoconstant array. |

## 5. A concurrent update to a pending row

These writers update a `PENDING` row and keep it `PENDING`: a priority
change, a sticky-pin release, a queue-pause shift, a cap raise, an owner
stamp, a shard move and a signal re-pend. Each one is rare. No writer
updates one row again and again, so no row starves. Some writers rewrite
many rows in one commit. Then each claim in flight on that head skips
those rows once.

On a release that skips (section 2), the claim then takes the next window
row, reads the deep window, or returns no task. It keeps its lock on the
skipped row until its transaction ends. A later poll reads the new
version. A priority change sends no notification, so the row can wait one
poll interval. The claim never hands out an ineligible row. Under
contention the claim order was already best effort. See
`docs/performance.md`, the fairness section.

A retry of an empty claim would close the gap. It costs a second claim
statement on every empty poll, so this change does not add it.

## 6. Six thinking hats

- **White (facts).** The refine is 83.6% of a claim. A primary-key probe is
  about 4.2 buffers. A TID probe is about 1. The scratch run read 68 against
  33 buffers for 31 rows.
- **Red (feeling).** The change is small and local. Nobody has to choose a
  new index or a new window size.
- **Black (risk).** The race in section 5. The planner can pick a plan that
  nobody tested. A text anchor in a splice can break.
- **Yellow (benefit).** The refine falls from about 4.2 to about 1 buffer
  per row. The deep window gets the same gain, about 3 buffers per deep row.
  There is no new write cost and no migration.
- **Green (other ideas).** Probe rows in key order and stop at the first
  row that locks. It reads about 1 row per claim, not 29. It changes the
  plan shape and needs an equivalence proof, so it is a follow-up.
- **Blue (process).** RED: a test that fails on the primary-key refine.
  GREEN: carry `ctid`, refine by `ctid`. REFACTOR: comments, shape tests,
  docs. Then a review from several angles.

## 7. Decision

Each window row carries its `ctid` as `seek_ctid`. The guard of the window
and of the deep window selects `seek_ctid`, and the refine tests
`harvest_task_queue.ctid = ANY(...)`. The full scan, the by-id claim and the
batched claim do not change.

## 8. Acceptance criteria

1. On the issue #1956 fixture at 32 and 1,000 pending rows, the window
   refine reads at most 1.5 buffers per row that it reads.
2. Every claim order, cap, held-claim and stale-statistics test passes.
3. The deep window refine is a TID scan at about 1 buffer per row at
   K = 32 and 64. `held_claims_on_one_head_keep_the_claim_off_the_full_scan`
   asserts it.
4. `docs/performance.md` records the before and after figures.

## 9. Not in scope

- Mechanisms 1, 2 and 3 of the issue.
- The `INSERT harvest_events` cost and the FK `FOR KEY SHARE` probe.

## 10. Follow-up: a drained backlog

Review found that `stale_statistics_keep_the_window_bounded` failed at
random. It failed on `trunk-dev` too.

### 10.1 Facts

- The test seeded 20 `PENDING` rows among 400K. `ANALYZE` samples 30,000
  rows, so it missed all 20 in about one run of five.
- When it misses them, the statistics show no `PENDING` row. A drained
  backlog gives the same statistics every time.
- The partial indexes keep the pages of the drained backlog. `VACUUM` sets
  their row count to 0. A later `VACUUM` with no dead rows does not scan
  them, so the count stays 0.
- The planner then estimates each head as empty. Each queue head sorted
  all of `idx_harvest_tq_live_created`. The pin head read it once more.
- On the test fixture, `trunk-dev` read 18,576 buffers, against 405 with
  fresh statistics. The cost grows with the backlog.
- A review of the first fix found a second plan. An equality on the kind
  and the new-start flag makes both columns constant. So
  `idx_harvest_tq_poll` gives a head its order with no sort, and filters
  those columns. On a scratch fixture of 64 queues of one kind each, it
  read 274,301 buffers with sorts off.

### 10.2 Brainstorming

| Option | Verdict |
|---|---|
| **`SET LOCAL enable_sort = off` in the claim** | Chosen. A head can then only use an index that gives its order. `bounded_queue_depths` does the same (issue #2356). |
| **A range on the new-start flag, first in the head order** | Chosen. Only `idx_harvest_tq_claim_seek` holds that column after the queue and the kind. |
| **Pins in a subquery, a range on the worker, first in its order** | Chosen. Only `idx_harvest_tq_sticky_poll` leads with the worker. |
| A state test that proves no other partial index | Not possible. `state = 'PENDING'` proves `state IN ('PENDING', 'RUNNING')`. |
| Change or drop `idx_harvest_tq_poll` | Rejected. It needs a migration on a hot table, and the full scan uses it. |
| Keep the statistics fresh | Not in the claim's control. An operator owns `ANALYZE`. |
| `enable_seqscan = off` | Rejected. It also changes the full-scan fallback, and no fixture here measures that. |

### 10.3 Reverse brainstorming

| Way to fail | Guard |
|---|---|
| Another index gives a head its order | `only_the_ordered_index_gives_a_head_its_order` drops both ordered indexes, and each head scan must then sort. It does not depend on costs. |
| A range selects other rows than the equality | Both columns are `NOT NULL`, so `x >= v AND x <= v` is `x = v`. |
| A sort that the claim needs disappears | Every plan of that step has the sort, so its cost does not change. The claim order tests and the random drains pass. |
| Another step gets worse | With fresh statistics, only two small aggregates change: they hash instead of sort. Every scan and join is the same. The plan has no merge join. `claim_seek_tests` and 25 claim suites pass. |
| The pin head returns other rows | It reads every live pin, as before. The outer `ORDER BY ... LIMIT` is the same. A review compared 80 seeded runs. |
| Postgres 18 counts disabled nodes, not costs | A plan with fewer disabled sorts still wins, so the result is the same. |

### 10.4 Six thinking hats

- **White.** On the test fixture, `trunk-dev` read 18,576 buffers and the
  fix reads 247. On its 64 queues of one kind each, 974,412 against 4,097.
- **Red.** One setting and three changes of text, each with a test.
- **Black.** A cached plan from a table of 10 rows or fewer can still pick
  a sequential scan for the refine. The first autoanalyze ends it.
- **Yellow.** No plan of a head depends on costs any more. The issue
  #1956 fixture with 20K terminal rows falls from 202 to 125 buffers.
- **Green.** A pin index ordered by the due time would let the pin head
  stop at 32 rows.
- **Blue.** RED: the test owns its database, turns autovacuum off and
  asserts the head plans. A second test drops the ordered indexes. GREEN:
  the setting, the ranges and the pin subquery.

### 10.5 Acceptance criteria

1. The test passes on each run, and asserts that the statistics show no
   `PENDING` row.
2. Each head scan reads `idx_harvest_tq_claim_seek` or
   `idx_harvest_tq_sticky_poll`, in both layouts of the test.
3. With both indexes dropped, each head scan sorts.
4. Every `claim_seek_tests` case passes.
