## Engine — Continuations before new starts; no work past a run deadline (issue #1824)

**Behavior change.** Two claim rules now apply by default. See
[`operations/claim-order.md`](../operations/claim-order.md).

**Continuation band.** At equal effective priority, the first workflow task
of a freshly admitted run sorts as if it were due 30 seconds later
(`queue::NEW_START_HANDICAP_SECS`). Under a backlog, activity tasks and
woken workflow tasks of running workflows therefore go first.

- The handicap is fixed. A new start that waits longer competes FIFO
  again, so continuations cannot starve it. No separate ageing term is
  needed.
- The term sorts after the effective priority, so an explicit priority
  still wins. Priority ageing below 30 seconds can lift an aged new start.
- Only a fresh admission yields. The start path sets the new column
  `harvest_task_queue.new_start`, except for a workflow retry. Child starts,
  continue-as-new, reset forks and DLQ redrives keep `FALSE`. This split is
  close to the admission gate's, but not identical.
- One SQL term, `queue::CLAIM_ORDER_DUE_SQL`, orders every claim variant:
  base, fenced, by-id, by-kind and batched. The batched keyset cursor now
  carries that term instead of `scheduled_at`.
- Cost: about 18 ms per claim at a 20k-row backlog. See
  `docs/performance.md`.

**Run deadline.** Every claim variant skips a task of a `RUNNING` run that
is past `deadline_at` or `chain_deadline_at`. The task stays `PENDING`, and
the claim takes the next eligible row.

- `queue::EXPIRED_RUNS_CTE_SQL` reads the expired runs once per claim, with
  the timeout scanner's own predicate. Two partial indexes serve it, and a
  hashed anti-join applies it. A `PAUSED` run is not in the set.
- The claim writes nothing for a skipped row. So it spends no attempt,
  rate-limit token or concurrency slot, and it takes no lock on the run row.
  A first design failed the row after the claim. Review found two defects
  in that design. The second row write fired the foreign-key check. That
  check took a run-row lock, which could deadlock with the scanner. The
  claim had also already debited a rate-limit token.
- A single-row claim can wait on its rate-limit bucket lock, and a deadline
  can pass during that wait. So the claim takes that lock first, then reads
  `clock_timestamp()`, and re-checks the run of its candidate. That re-check
  gates both the debit and the claim.
- The batched claim runs its scan and each candidate attempt in one
  transaction, where `NOW()` stays fixed. So the attempt re-checks the run
  deadline on its own `clock_timestamp()` read. That re-check gates both the
  rate-limit debit and the claim.
- The scanner then times out the run. It now prefixes each open task's
  error with `queue::DEADLINE_EXCEEDED_ERROR` (`deadline_exceeded`).

**Migration.** `20261004162927_harvest_task_queue_new_start` adds
`new_start BOOLEAN NOT NULL DEFAULT FALSE`, with a 5-second lock timeout.
No table rewrite, no index. No `WorkflowEvent` variant, no replay impact.
`harvest_events` is not written. Shard-rebalance activation now merges
`new_start: false` under a staged row. A row staged before the migration
therefore restores, where it would have failed the NOT NULL check.

**API.** `EnqueueParams` and `models::NewTaskQueueItem` gain the public
field `new_start`. `models::TaskQueueItem` gains `new_start`, with a serde
default.

**Limits.** The handicap has no setting. Dispatch-channel hints keep their
`priority, scheduled_at` order. The SQLite and Redis-queue backends do not
use the band.

**Tests.**

- `claim_continuation_priority_tests` (12 tests) covers:
  - a single slot claims continuations first, on the single-row, batched
    and kind-filtered paths;
  - aged new starts progress under a growing continuation stream, and under
    simulated time;
  - the handicap expires;
  - explicit priority wins, and both ageing cases behave as documented;
  - spawned runs and retries do not yield, and the start path sets the
    marker.
- `claim_run_deadline_tests` (13 tests) covers:
  - workflow, activity and chain deadlines, each skipped and then recorded
    by the scanner;
  - expired rows do not block the live row behind them;
  - future and absent deadlines, and paused runs;
  - the by-id, kind-filtered and batched paths;
  - a skipped task spends no rate-limit token;
  - the batched attempt re-checks the run on a fresh clock;
  - a deadline that passes during a bucket lock wait stops the claim.
- `shard_rebalance_db_tests` adds a staged row without the key.
- Unit tests pin the SQL terms in every claim variant.
