## Engine — Worker drain releases its claims (issue #1813)

**Breaking default.** `WorkerConfig::default()` now sets `shutdown_timeout`
to `DEFAULT_SHUTDOWN_TIMEOUT` (25 s). Before, it was 30 s, the same as the
Kubernetes default `terminationGracePeriodSeconds`. The platform could then
kill the pod at the moment the drain gave up. Keep `shutdown_timeout` at
least 5 s below the platform grace period.

**What shipped.** Before, the drain only logged "some tasks may still be
running" at its deadline. Every claim then waited for orphan reclaim. A
dispatch body was a detached `tokio::spawn`, so a handler could also outlive
the drain. Now:

- A `TaskTracker` keeps each dispatch body. The drain waits for the tracker,
  then for the permits, as before.
- A task claimed but not started when shutdown begins gives its claim back
  at once. The new fenced write `queue::release_unstarted_claim` sets the row
  `PENDING`, restores `attempt` and clears a sticky pin on a row with no
  session. It keeps `error`, `crash_strikes` and the capability-miss
  counters. The worker also refunds a claim-time rate-limit debit.
- One join window before the deadline, the drain cancels running
  activities. The join window is `cancellation_grace_period`, capped at half
  the drain. A later remote deadline cannot undo the cancel. An activity
  whose setup ends after the cancel never starts its handler. It goes back
  at once, whatever its retry policy says.
- The drain cancels the activity context only. The heartbeat flusher runs
  until the handler returns. The cancel stops the handler's own heartbeats,
  so the worker then re-sends the last checkpoint at
  `heartbeat_timeout / 3`. The heartbeat timeout therefore cannot fail a
  handler that ignores the cancel.
- A cancelled handler that returns a retryable error gives its claim back
  through `queue::requeue_claimed_task_for_retry`, due at once. The stored
  error is `worker shutdown: <handler message>`. The attempt counts. The
  release skips the retry delay and the attempt cap, as orphan reclaim does,
  so a deploy never fails an activity. A handler that returns `Ok`, or a
  non-retryable error, takes the normal path.
- A handler that ignores the cancel keeps its claim. The drain never drops
  it. Its late result still lands through the #1789 fence.
- While such a handler runs, the worker keeps its lease alive through the
  new `workers::touch_worker_liveness`, even after `run` returns. A host
  process that outlives `run`, such as an embedded runtime, therefore never
  lets a peer start a second copy. While the row is not `Active`, the
  refresh also clears its queues, so a capability-miss lookup does not count
  the worker as a capable peer. A replacement worker with the same id
  registers as `Active` and keeps its own queues.
  If the worker row is gone, the keeper
  restores it through the new `workers::restore_stopped_worker_row`. The
  restored row is `Stopped` and lists no queue or shard, so it claims no
  coverage. If the process exits, orphan reclaim recovers the task.
- The kept lease hides every claim of the worker from orphan reclaim. So
  each lease refresh also gives back, through the new fenced
  `queue::release_abandoned_claim`, each `RUNNING` claim whose dispatch
  body ended after shutdown began. A failed release or result write leaves
  such a claim. The match uses the task id, `attempt` and `started_at`, and
  the write is fenced on all three. A replacement worker with the same id
  writes a new `started_at` on each claim, so the keeper never touches its
  claims.

**Safety.** A claim is released only when no handler for it can still run:
the handler never started, or it returned. The release is fenced on the
claim epoch `(worker_id, attempt)`. A never-started task restores `attempt`,
because no writer for that epoch exists. A joined task keeps it, so an old
epoch never matches a later claim.

**Invariants.** No migration. No new `WorkflowEvent` variant. No write to
`harvest_events`.

**Scope.** The kept lease lasts only while a claim of the drained worker is
current. A handler whose claim is lost is dropped after the grace period, as
before. Two live workers with the same id share one lease row, so each hides
the other while both run, as with the normal heartbeat. Running workflow
tasks are not cancelled. `workflow_task_timeout`
bounds them, and #1184 fences their writes. A drained attempt still counts as
a failed attempt in `harvest.activity.attempts` and `harvest.activity.failed`.
Its release counts as one retry in `harvest.activity.retries`.
Issue #1552, which this issue lists, shipped in #1778. The plugin's default
drain deadline and the connector's `shutdown_timeout` now use
`DEFAULT_SHUTDOWN_TIMEOUT`.

**Upgrade.** See `docs/upgrading/0.7.0.md`, section 1.6.

**Tests.**

- `chaos_tests::drain_hold::chaos_repro_1813_drain_releases_a_claim_that_never_started`
  uses the new chaos point `WORKER_DISPATCH_BEFORE_START`.
- `drain_release_tests::release_unstarted_claim_restores_the_claim_and_is_fenced`.
- `drain_release_tests::drain_joins_a_cooperative_activity_and_a_peer_retries_it`.
- `drain_release_tests::drain_keeps_the_claim_of_an_activity_that_ignores_the_cancel`.
- Unit tests for `drain_cancel_at` and the default.
