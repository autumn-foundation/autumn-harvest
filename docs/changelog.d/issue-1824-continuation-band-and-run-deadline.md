## Engine — Continuations before new starts; no work past a run deadline (issue #1824)

**Behavior change.** Two claim rules now apply by default. See
[`operations/claim-order.md`](../operations/claim-order.md).

**Continuation band.** At equal priority, the first workflow task of a
freshly admitted run sorts as if it were due 30 seconds later
(`queue::NEW_START_HANDICAP_SECS`). Under a backlog, activity tasks and
woken workflow tasks of running workflows therefore go first.

- The handicap is fixed. A new start that waits longer competes FIFO
  again, so continuations cannot starve it. No separate ageing term is
  needed.
- The term sorts after `priority`, so an explicit priority still wins.
- Only a fresh admission yields. The start path sets the new column
  `harvest_task_queue.new_start`, except for a workflow retry. Child
  starts, continue-as-new, reset forks and DLQ redrives keep `FALSE`. This
  is the split the admission gate and load shedding already use.
- One SQL term, `queue::CLAIM_ORDER_DUE_SQL`, orders every claim variant:
  base, fenced, by-id, by-kind and batched. The batched keyset cursor now
  carries that term instead of `scheduled_at`.
- The claim already sorts on a `CASE` key (issue #1177). The new term adds
  no sort that an index could have saved.

**Run deadline.** The shared post-claim recheck now fails a task whose
`RUNNING` run is past `deadline_at` or `chain_deadline_at`. The task is
`FAILED` with `queue::DEADLINE_EXCEEDED_ERROR` and does not run.

- The predicate is the timeout scanner's own. A `PAUSED` run is not
  matched, because a resume moves its deadline forward.
- The check reads the run row and never locks it. The scanner locks the
  run and then the tasks, so a lock here could deadlock.
- The single-row claim then claims again, up to 8 times. The batched
  claim tries its next candidate. The by-id claim returns `None`, and the
  caller acks the reference.
- The scanner still times out the run. It rewrites only open rows, so the
  task keeps its error.

**Migration.** `20261004162927_harvest_task_queue_new_start` adds
`new_start BOOLEAN NOT NULL DEFAULT FALSE`. No table rewrite, no index. No
`WorkflowEvent` variant, no replay impact. `harvest_events` is not written.

**Limits.** The handicap has no setting. Dispatch-channel hints keep their
`priority, scheduled_at` order. The SQLite and Redis-queue backends do not
use the band.

**Tests.** `claim_continuation_priority_tests` (8 tests): a single slot
claims continuations first, on both the single-row and batched paths. Aged
new starts progress under a growing continuation stream. The handicap
expires. Explicit priority wins. Spawned runs and retries do not yield.
The start path sets the marker. `claim_run_deadline_tests` (9 tests):
workflow, activity and chain deadlines; an expired row does not block the
live row behind it; future and absent deadlines; paused runs; the by-id
and batched paths; and the scanner keeps the outcome. Unit tests pin the
SQL term in every claim variant and the deadline statement's guards.
