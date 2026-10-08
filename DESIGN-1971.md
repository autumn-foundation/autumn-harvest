# Design — Issue #1971: a flat-cost default claim path

The default claim statement scans and sorts every due `PENDING` row of the
polled queues. Claim cost therefore grows with backlog depth. At a 100K
backlog the sort spills to disk, and a claim takes about 250 ms.

This change adds a bounded, index-ordered **seek window** in front of the
existing candidate scan. A guard proves that the window holds the row that
the full scan would pick. When the guard cannot prove it, the same statement
runs the full scan, as before.

**One migration (one index). No new `WorkflowEvent` variant. No new bind.
No change to the claim order.** The claim transaction also turns JIT off and
forces the generic plan, with `SET LOCAL` (§1.6).

---

## 0. Planning record

### 0.1 Research summary

Sources: Temporal, Hatchet, River, Oban, graphile-worker, Solid Queue, Que,
PgQ, pgmq, DBOS, Postgres source and docs. URLs are in the PR body.

| Finding | Effect on this design |
|---|---|
| Every fast engine pins one queue by equality, orders by the index key, and applies no residual predicate at claim time. | Read one ordered index range per queue. Apply the residual gates to a small set only. |
| `queue_name = ANY($2)` with more than one value cannot return a global order. | Use `unnest($2)` with a `LATERAL` top-N per queue. |
| `LockRows` does not pass the `LIMIT` bound to the `Sort` below it. The sort is then unbounded and spills. | Sort only the window. The window is small. |
| `timestamptz + interval` is `STABLE`. The 30 s handicap cannot be an index expression on Postgres 12 to 15. | Split each queue into two heads on the immutable `(new_start AND attempt = 0)`. Inside one head, the due order is the `scheduled_at` order. |
| Temporal and Hatchet keep ineligible work out of the ready index. | Long-term direction. Out of scope here (§4). |

### 0.2 Brainstorm — how can claim cost become flat?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Wire `claim_task_batched` in as the default. | Rejected. Its candidate scan is still a full scan and sort. Assay 0005 and its own docs say so. |
| B2 | Add a stored `claim_due_at` column with an index. | Rejected. `ADD COLUMN ... GENERATED STORED` rewrites the hot table. The immutable `date_add` needs Postgres 16. Harvest supports 12+. |
| B3 | A window per queue, refine, then a keyset loop from Rust. | Rejected. One round trip per page. The loop and its cursor are a second claim engine to keep correct. |
| B4 | A window per queue and kind of head, an exactness guard, and a lazy fallback to the full scan in the same statement. | **Adopted.** One round trip. The claim order is unchanged by construction. The downstream claim CTEs are unchanged. |
| B5 | Separate `READY`, `BLOCKED` and `SCHEDULED` states or tables. | Deferred (§4). It changes every writer of `harvest_task_queue`. |
| B6 | An in-process matcher per queue, as Temporal does. | Deferred (§4). It is a new subsystem. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A storm of new starts hides a continuation. That breaks the #1824 order. | One head per queue for continuations and one for new starts. Test: `a_continuation_behind_a_new_start_storm_claims_first`. |
| R2 | A row pinned to this worker sits deep in the backlog and is not seen. | A third head reads this worker's live pins through `idx_harvest_tq_sticky_poll`. Test: `a_row_pinned_to_this_worker_claims_first_from_deep_in_the_backlog`. |
| R3 | The window head of one queue is ineligible. A lower-priority queue then wins. | The guard rejects a row that sorts after the last row of a full head. Then the full scan runs. Test: `an_eligible_row_behind_a_saturated_head_beats_a_lower_priority_queue`. |
| R4 | Priority ageing stops working. | Ageing changes the order at claim time, so the window cannot serve it. With `$4 > 0`, the window is empty and the full scan runs. Test: `ageing_lifts_an_old_row_over_a_deep_high_priority_backlog`. |
| R5 | A window with no full head returns nothing, but work exists. | A head that is not full holds every due row of its queue and kind. So an empty result is exact. The randomized drain test checks it. |
| R6 | The fallback scan always runs, so the cost does not drop. | The fallback is gated by a one-time filter. `EXPLAIN` shows `never executed` when the window wins. The depth test measures it. |
| R7 | The concurrency-count CTE still scans the whole backlog. | The window path reads its own count CTE, scoped to the window rows. The full-scan CTEs run only with the fallback. |
| R8 | The new index slows enqueue and claim writes. | One partial B-tree on `PENDING` rows. Every state change already leaves `PENDING` and is not HOT. The cost is measured in `docs/performance.md`. |
| R9 | The migration blocks the hot table. | `lock_timeout = '5s'`, a guarded build, and an out-of-band `CONCURRENTLY` recipe (issue #1810). |
| R10 | The fence, kind and by-id splices break or claim the wrong row. | The fence and kind splices apply to both candidate scans. The by-id claim keeps the full-scan form. Shape tests count each anchor. |
| R11 | A double claim, or a concurrency-cap overrun. | The window only selects the candidate. `FOR UPDATE SKIP LOCKED`, the advisory-lock recheck and the `claimed` update are unchanged. The existing concurrency suites run. |
| R12 | A head gate that the planner misjudges makes it read the whole head. | Found in measurement: with one activity type, the plain activity-name gates estimate near zero rows. The head scans write them as one `CASE`. Test: `a_single_activity_type_backlog_keeps_the_window_bounded`. |
| R13 | JIT or planning time eats the gain. | Found in measurement: the plan carries the full-scan estimate, so JIT ran on every claim. A custom plan costs about 6 ms. The claim sets `jit = off` and `force_generic_plan` for its own transaction. |

### 0.4 Six thinking hats — B4

| Hat | Notes |
|-----|-------|
| White | Prototype at 100K rows: the current statement takes 248 ms and writes 1,720 temp blocks. The window statement touches about 700 buffers and takes about 2 ms. At 1K rows, the current statement touches about 80 buffers. The window touches 300 to 500. |
| Red | "Same order, bounded work" is easy to trust. The fallback keeps the old behaviour as a safety net. |
| Black | (1) A saturated head makes every claim fall back. The cost is then today's cost plus the window. (2) At a shallow backlog, the window reads more buffers than the full scan. Its execution time is still lower. (3) Planning time grows, because the statement holds two candidate scans. A cached generic plan removes it. (4) Future-dated rows in a higher priority band are walked past in the index. |
| Yellow | The order is unchanged, so fairness does not change. The downstream CTEs are byte-identical, so the exactly-once, `SKIP LOCKED` and advisory-lock arguments carry over. The #1215 spill disappears whenever the window wins. |
| Green | B3, B5 and B6. Row-local gates move into the head scans, so rows pinned to other workers, paused activities and saturated activity types do not use window slots. |
| Blue | Red phase: the depth test, the #1215 test and the shape tests fail. Green phase: the migration and the query. Refactor phase: macros, docs, measurement. Then a multi-angle review. |

---

## 1. Design

### 1.1 The index

```sql
CREATE INDEX idx_harvest_tq_claim_seek ON harvest_task_queue
    (queue_name, (new_start AND attempt = 0), priority DESC, scheduled_at)
    WHERE state = 'PENDING';
```

Equality on the first two keys gives one ordered range per queue and kind of
head. Inside the continuation head, the due time is `scheduled_at`. Inside the
new-start head, the due time is `scheduled_at + 30 s`. Both orders match
`priority DESC, due ASC`.

### 1.2 The window

`seek_heads` reads, for each polled queue, the first `CLAIM_SEEK_WINDOW` due
rows of each head, in index order. A third head reads this worker's live pins.
Each head scan also applies the row-local gates. These gates read only the
row and constant arrays:

- the queue pause (the whole head is skipped);
- the activity pause, the `$6` ineligible names and the saturated types;
- a live pin to another worker, and a session pin;
- an expired `schedule_to_close_at`.

Every row these gates skip is ineligible. So the guard argument in §1.3 holds.
The three activity-name gates sit in one `CASE`. The planner cannot read
their arrays at plan time, and in their plain form it can estimate that no
row passes. It then reads the whole head. A `CASE` gets a fixed default
estimate.

`seek_bounds` keeps the last row of each full head.

### 1.3 The guard

A row `x` from the window may be the candidate only when no row outside the
window can sort before it:

- For each full queue head with last row `b`: `x` is pinned to this worker,
  or `x` sorts at or before `b` by `(priority DESC, due ASC)`.
- If the pin head is full with last row `s`: `x` is pinned to this worker and
  sorts at or before `s`.

A row outside a head sorts at or after the last row of that head. So the best
window row that passes the guard is the best eligible row of the whole
backlog. Ties may go either way, as before.

### 1.4 The fallback

`legacy_candidate` is the full candidate scan, unchanged. It has a one-time
filter: the window found no candidate, and a head was full or ageing is on.
Otherwise the scan never runs. `candidate` is the union of the two. The
`fresh_now`, `rate_limit_debit` and `claimed` CTEs are unchanged.

### 1.5 Splices

- Fence (#954): `CROSS JOIN fence` goes into both candidate scans.
- Kind (#1787): the kind predicate goes into both candidate scans and into
  every head scan.
- By id (#1312): the by-id claim names one row. It keeps the full-scan form.

### 1.6 Planner settings

The claim transaction sends `SET LOCAL jit = off; SET LOCAL plan_cache_mode =
force_generic_plan` as one batch, before the claim.

- The plan carries the estimated cost of the full scan, even when the
  one-time filter skips it. That estimate passes `jit_above_cost` at depth.
  JIT then compiled about 330 functions per claim, about 250 ms.
- A custom plan costs about 6 ms to plan. The generic plan has the same
  shape, and Postgres builds it once per connection.
- `SET LOCAL` ends with the transaction, so the session keeps its settings.

## 2. Test plan

| Phase | Test | Expected in red | Expected in green |
|---|---|---|---|
| Red | `claim_buffers_stay_flat_from_1k_to_100k_pending_rows` | Fail: buffers grow, temp blocks at 100K. | Pass. |
| Red | `a_large_activity_pause_array_does_not_spill_the_claim_at_100k` | Fail: the sort spills. | Pass. |
| Red | Shape tests for `seek_heads`, the guard and the fallback | Fail: no window. | Pass. |
| Red, added in green | `a_single_activity_type_backlog_keeps_the_window_bounded` | Fail with plain head gates: 1,393 to 10,916 buffers. | Pass: 668 to 638. |
| Both | Order tests and the randomized drain | Pass. | Pass. |
| Both | Existing claim suites: concurrency, pause, build routing, DR fence, run deadline, continuation priority, batched | Pass. | Pass. |

## 3. Measurement

`docs/performance.md` gets a section with claim buffers and latency at 1K,
10K and 100K, before and after. It also states the fairness change (none
with ageing off) and the fallback cases.

## 4. Follow-ups (not in this change)

1. Move parked work out of the ready index: `SCHEDULED` rows, rows blocked on
   a concurrency key, rows over a rate limit, and paused rows. The claim then
   needs no fallback.
2. A narrow ready table with insert-on-ready and delete-on-claim.
3. An in-process matcher per queue partition with sync match, as Temporal
   does.
4. An exact window for priority ageing: one head per priority level.
