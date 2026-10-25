# Design: issue #2308, the owner decider takes tasks in claim order

[ADR 0009](docs/adr/0009-decision-owner.md) decision 13 describes the decider.
[`claim-order.md`](docs/operations/claim-order.md) defines the order. This
change gives the decider that order.

**One migration (an index). No new `WorkflowEvent` variant. No replay impact.**

## 0. Planning record

### 0.1 Facts

- The ready set was a FIFO queue. A scan read `scheduled_at` only, with one
  `LIMIT` per partition. A wake carried an execution id and no key.
- A by-execution take claims one row, so it cannot compare rows. The order
  must come before the take.
- The claim sorts by priority, then by the due time with the 30 s handicap.
  Its seek window reads two heads per queue (new starts, continuations) from
  `idx_harvest_tq_claim_seek`. Ageing makes it fall back to a full scan.
- Ordering matters only when rows compete for permits.

### 0.2 Brainstorm

| # | Idea | Verdict |
|---|------|---------|
| B1 | Sort the ready set by the claim key. | **Adopted.** |
| B2 | The scan returns keys with each row. | **Adopted.** |
| B3 | Read a hint's key with one batched statement. | **Adopted.** |
| B4 | Skip that statement when free permits cover every hint. | **Adopted.** A saturated owner needs it. An idle owner does not. |
| B5 | Two heads per partition, as the claim has. | **Adopted.** It bounds the scan and keeps the merge exact. |
| B6 | One new index `(partition, start kind, priority DESC, scheduled_at)`. | **Adopted.** Keep the old index for the next due time. |
| B7 | Apply ageing in the scan. | Rejected. Ageing needs every pending row. |
| B8 | Run no decider when `priority_aging_secs` is set. | **Adopted.** The issue names it as the interim option. |
| B9 | Gate the decider off for any priority. | Rejected. Every row has a priority. |
| B10 | Add sticky affinity and the fairness lag. | Rejected. Fairness keys already disable the decider. Sticky pins are an activity feature. |

### 0.3 Reverse brainstorm

| # | How to cause harm | Counter-measure |
|---|-------------------|-----------------|
| H1 | A scan reads a whole backlog. | Two heads, a limit per head. Ageing runs no decider. `EXPLAIN` on 150k rows: 413 buffers. |
| H2 | The key lookup adds a statement per decision. | Skipped when free permits cover all hints. |
| H3 | A failed lookup loses a wake. | Queue the hints unranked. The scan corrects them. |
| H4 | A stale key reorders a row wrongly. | Each scan replaces the key of a queued row. |
| H5 | A full ready set drops the best row. | Drop the worst entry for a better one, never the reverse. |
| H6 | A high-priority row waits in the skip list. | A wake ends the backoff. A scan skips only queued, in-flight and backing-off rows. |
| H7 | A new index slows task writes. | It holds pending workflow rows only. The upgrade guide gives the concurrent build. |
| H8 | A scan misses a new start by 30 s of handicap. | The merge sorts by the handicapped due time. A test proves the 20 s and 90 s cases. |

### 0.4 Six hats

- **White.** Section 0.1. The issue asks for two tests: a high-priority row
  before an older backlog, and a continuation before a new start.
- **Red.** A second ordering rule in the decider feels risky. The SQL uses the
  claim's own due-time text, and one test pins both.
- **Black.** Section 0.3.
- **Yellow.** Rule 1 of issue #1824 holds again for workflow tasks in owner
  mode. Idle owners pay nothing.
- **Green.** Ageing could use a bounded window later, if a use case needs it.
- **Blue.** Red tests first (scan order, key lookup, ageing gate, saturated
  owner). Then the SQL, the index and the state. Then docs and a review from
  several angles.

## 1. Change

1. Migration `20261024184859_harvest_owner_decider_order`.
2. `queue.rs`: `owner_scan` returns ranked rows. New `owner_keys`.
3. `worker.rs`: a sorted ready set, a key lookup, the ageing gate.
4. Docs: ADR 0009 decision 13, `claim-order.md`, the upgrade guide, a
   changelog fragment.

## 2. Test plan

| Phase | Test | Red | Green |
|---|---|---|---|
| Red | `the_owner_scan_returns_a_high_priority_row_before_an_older_backlog` | Fail: oldest first. | Pass. |
| Red | `the_owner_scan_returns_a_continuation_before_a_new_start` | Fail. | Pass. |
| Red | `the_key_lookup_ranks_hinted_executions` | Fail: no keys. | Pass. |
| Red | `a_worker_with_priority_ageing_keeps_the_owned_claim` | Fail: 0 claims, the decider ran. | Pass. |
| Red | `a_saturated_owner_takes_a_high_priority_run_first` | Fail under FIFO order. | Pass. |
| Unit | `decider_state_*` (claim order, full queue, unkeyed hints) | Compile error. | Pass. |
