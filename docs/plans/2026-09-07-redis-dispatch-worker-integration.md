# Redis dispatch: wire `autumn-harvest-redis` into the worker (issue #1312)

**Status:** implementation plan. Supersedes the deferral in
`docs/rnd/2026-09-03-redis-queue-worker-integration-deferral.md`; issue #1312
is the dated, owned trigger that record asked for.

## 1. Decision in one paragraph

Postgres keeps every `harvest_task_queue` row and stays the only source of
truth. Redis Streams becomes the **dispatch channel**: after a row becomes
claimable, the engine publishes a small reference entry (task id, queue,
due time) to a per-queue stream. A worker reads references from the stream,
claims the named row in Postgres with the full claim predicate, then acks the
entry. Every write to workflow state and history stays in the Postgres
transactions that exist today. This is the architecture Temporal uses: the
matching service holds tasks in memory and the persistence layer is the
durable record, so a lost matching node never loses a task.

## 2. Why not replace the Postgres queue

The reverse brainstorm (section 5) makes the case. The Postgres row is not
only a queue slot. It carries thirteen claim gates (queue pause, activity
pause, concurrency cap, rate limit, sticky affinity, session pin, build
routing, capability match, DR fence, execution pause, circuit breaker,
schedule-to-close, due time), the park/wake cycle for workflow tasks, the
`(worker_id, crash_strikes)` claim token, and the orphan reclaim path. A
Redis-only queue would either re-implement all of that or lose it, and Redis
without persistence would lose tasks on restart. The reference-dispatch shape
keeps every guarantee and removes the one cost the assays measured: the
backlog scan and sort on every claim.

## 3. Brainstorm: candidate shapes

| # | Shape | Kept? | Reason |
|---|-------|-------|--------|
| A | Redis stream carries the full task envelope; Postgres row removed | no | Loses gates, park/wake, durability. |
| B | Redis stream carries a task-id reference; worker claims the row by id | **yes** | Keeps Postgres as truth, removes the backlog scan. |
| C | Redis only as a wake signal (replace LISTEN/NOTIFY) | no | No throughput change; the claim still scans the backlog. |
| D | Transactional outbox table polled into Redis | partial | The reconciler sweep in B is this outbox, without a new table. |
| E | Ack the stream entry after the completion commit | no | Adds no durability over B (Postgres owns the lease) and couples PEL visibility to activity duration. |
| F | Ack the stream entry right after the Postgres claim commit | **yes** | The claim commit is the only Postgres write the entry exists to trigger. |
| G | One stream per (queue, priority) | later | Priority order inside Redis is best effort in v1; the reconciler publishes in priority order. |
| H | Blocking `XREADGROUP` on a dedicated connection | **yes** | Low idle cost, low latency, one round trip for all queues. |
| I | Fallback to the Postgres claim path when Redis is unreachable | **yes** | Availability equals today's path when Redis is down. |

## 4. Mechanism

### 4.1 Publish

`dispatch::record_hint` runs wherever `notify::notify_task_enqueued` runs.
Inside a buffering scope the hint waits in a task-local buffer; the owner of
the transaction flushes the buffer after commit. Outside a scope the hint goes
to a bounded background publisher task that batches and pipelines `XADD`. The
scoped owners are the workflow persist flow in `worker.rs`, the workflow start
transaction in `execution.rs` and `handle.rs`, and the transaction owners in
`signal.rs`, `external_task.rs`, `sessions.rs`, `dlq.rs`, `reset.rs` and
`cross_shard_child.rs`. A nested owner leaves its hints with the outermost one.

A publish is idempotent per task id and keyed on `scheduled_at`. The Redis
implementation keeps a marker key that stores the held reference's due time.
A hint with the same `scheduled_at` is a no-op that refreshes the marker. A
hint with a different `scheduled_at` means the row changed, so the reference
moves to the new due time and its redelivery count resets. A released
reference keeps the row's `scheduled_at`, so a reconcile republish never
disturbs its backoff, while a wake or a retry does move it.

### 4.2 Consume

Under Redis dispatch `Worker::poll_once` reads up to `n` references with one
blocking `XREADGROUP` across all served queues, where `n` is the free
concurrency. For each reference it runs `queue::claim_task_by_id_on_shard`,
which is the existing claim statement plus one predicate on `id` in the
candidate CTE and one in the concurrency pending-keys CTE. Outcomes:

| Row state | Action |
|-----------|--------|
| claimed | ack the entry, dispatch the task |
| `PENDING`, due, gated | release the entry with exponential backoff, capped |
| `PENDING`, not yet due | release the entry until the due time |
| `RUNNING` and parked, or absent | release 50 ms, three times, then ack (a wake or an insert may be in flight) |
| `RUNNING` and owned, or terminal | ack the entry |

### 4.3 Reconcile

Every `dispatch_reconcile_interval` the worker reads due `PENDING` rows for its
queues in `(priority DESC, scheduled_at ASC)` order, bounded by a batch size,
and publishes them. The marker keys make this cheap when the stream already
holds the rows. This sweep is the durability floor: a Redis restart, a lost
entry, a dropped hint, or a crash between commit and publish all converge
through it. It is the Redis analogue of Temporal's matching reload from
persistence.

### 4.4 Maintain

Every `dispatch_poll_interval` the worker promotes due delayed entries. Every
`visibility_timeout / 2` it recovers entries left in the pending entries list
by a crashed peer. A recovered entry for a row that is no longer `PENDING`
is acked on delivery, so a crash between the claim commit and the ack never
duplicates work.

### 4.5 Crash matrix

| Crash point | Postgres | Redis | Recovery |
|-------------|----------|-------|----------|
| before claim commit | row `PENDING` | entry in PEL | `recover_pending` re-adds; claim succeeds once |
| after claim commit, before ack | row `RUNNING` by dead worker | entry in PEL | poison-pill reclaim re-pends the row; reconciler republishes; the stale entry is acked as a no-op |
| after completion commit | row terminal | no entry | nothing to do |
| after publish, Redis restarts | row `PENDING` | entry lost | reconciler republishes |

## 5. Reverse brainstorm: how to make this lose or duplicate work

| Attack | Defence |
|--------|---------|
| Publish before commit, worker claims a row that is not visible | absent rows get three short retries; the reconciler is the floor |
| Redis loses every entry | reconciler sweep |
| Marker key leaks and blocks republish forever | markers expire (`dedupe_ttl`) and refresh on every publish; a changed `scheduled_at` overrides the held entry |
| A signal arrives while the row is parked in the delayed set | the wake writes a new `scheduled_at`, so the publish moves the entry forward |
| Two workers read the same entry | `XREADGROUP` delivers once; the claim is `FOR UPDATE SKIP LOCKED` |
| A worker crashes between claim and ack | PEL recovery; the redelivered reference finds the row `RUNNING` and acks |
| A gated row cycles every poll | exponential backoff on release, capped at `dispatch_release_backoff_cap` |
| Redis down or hung | every channel call has a timeout; the claim falls back to the Postgres poll path for that iteration |
| Multi-shard worker reads a reference for another shard's pool | v1 rejects Redis dispatch on sharded runtimes at startup: `HarvestRunner::start` refuses before it installs the channel, and `Worker::new` repeats the check. Config validation cannot see the resolved pool. The hint carries a shard slot for the follow-up |
| Sticky affinity gate rejects every non-pinned worker | release with backoff; affinity is a cache hint, not a correctness rule |
| `attempt` burns on redelivery | the by-id claim is the only `PENDING -> RUNNING` writer, and a gated miss never increments it |

## 6. Six hats

- **White (facts).** Assay #2 measured 18,933 claims/s standalone against 29/s
  for the Postgres path at a 10,000-row backlog. The worker has one claim
  seam (`poll_once`) and one post-commit seam (`WORKER_AFTER_OUTER_COMMIT`).
  The Redis crate has no CI coverage today.
- **Red (feelings).** The deferral record called `worker.rs` the hottest file
  in the repo. The change must stay small there: one config field, one branch
  in `poll_once`, flush calls after commits.
- **Black (risks).** Priority order and sticky affinity degrade to best effort
  under Redis dispatch. Multi-shard is out of v1. The deployment-shaped assay
  may kill the 10,000 tasks/s claim on a 4-CPU box; the docs must then say so.
- **Yellow (benefits).** No new table, no migration, no change to history
  writes, no change to the Postgres path when Redis is off. Durability floor
  equals Temporal's.
- **Green (alternatives).** Per-priority streams and per-worker sticky streams
  are natural follow-ups on the same seam. The `TaskDispatch` trait admits an
  in-memory implementation for tests and a future NATS or SQS implementation.
- **Blue (process).** Red, green, refactor. Tests first in each package.
  Pre-registration for the assay lands in its own commit before any apparatus.

## 7. Work packages

| Package | Files | Tests first |
|---------|-------|-------------|
| Core seam | `dispatch.rs`, `queue.rs` (`claim_task_by_id_on_shard`, probe, reconcile query), `notify.rs`, `execution.rs`, `worker.rs`, `builder.rs` | unit tests for buffering and backoff; DB tests with `MemoryDispatch` |
| Redis implementation | `autumn-harvest-redis/src/dispatch.rs`, `redis_queue.rs` | Redis tests for dedupe, reschedule, batch read, release, recovery; end-to-end and process-kill tests |
| Operator wiring | plugin `config.rs`, `runner.rs`, plugin feature `redis`, CI steps, docs | config parse and validation tests |
| Assay #8 | `docs/rnd/...-preregistration.md`, `docs/assays/0006-*.md`, apparatus | pre-registration commit first |

## 8. Acceptance criteria map

| AC | Evidence |
|----|----------|
| Operator can configure Redis dispatch end to end | `[harvest.redis]` config, plugin wiring, end-to-end test with real Postgres and Redis |
| Crash between the Postgres commit and the Redis ack loses nothing and duplicates nothing | process-kill test: the child worker aborts after the claim commit and before the ack; the parent verifies one execution and a clean stream |
| Deployment-shaped throughput assay gates the 10,000 tasks/s claim | assay #8 with a pre-registered kill line and a matched Postgres control in the same run |
