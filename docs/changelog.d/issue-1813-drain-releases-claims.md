## Engine — Worker drain releases its claims (issue #1813)

**Breaking default.** `WorkerConfig::default()` now sets `shutdown_timeout`
to `DEFAULT_SHUTDOWN_TIMEOUT` (25 s). Before, it was 30 s, the same as the
Kubernetes default `terminationGracePeriodSeconds`. The platform could then
kill the pod at the moment the drain gave up. Keep `shutdown_timeout` at
least 5 s below the platform grace period.

**What shipped.** Before, the drain only logged "some tasks may still be
running" at its deadline. Every claim then waited for lease expiry. A
dispatch body was a detached `tokio::spawn`, so a handler could also outlive
the drain. Now:

- A `TaskTracker` keeps each dispatch body. The drain waits for the tracker,
  then for the permits, as before.
- A task claimed but not started when shutdown begins gives its claim back
  at once. The new fenced write `queue::release_unstarted_claim` sets the row
  `PENDING` and restores `attempt`. It keeps `error`, `crash_strikes` and the
  capability-miss counters. It refunds a claim-time rate-limit debit.
- One join window before the deadline, the drain cancels running
  activities. The join window is `cancellation_grace_period`, capped at half
  the drain.
- A cancelled activity whose handler returns an error gives its claim back
  through `queue::requeue_claimed_task_for_retry` with no delay and the error
  `worker shutdown: ...`. The attempt counts. The release skips the retry
  policy, as orphan reclaim does, so a deploy never fails an activity. A
  handler that returns `Ok` completes as usual.
- A handler that ignores the cancel keeps its claim. The drain never drops
  it. Its late result still lands through the #1789 fence. If the process
  exits, the lease recovers the task.

**Safety.** A claim is released only when no handler for it can still run:
the handler never started, or it returned. The release is fenced on the
claim epoch `(worker_id, attempt)`. A never-started task restores `attempt`,
because no stale writer for that epoch exists. A joined task keeps it, so an
old epoch never matches a later claim.

**Scope.** Running workflow tasks are not cancelled. `workflow_task_timeout`
bounds them, and #1184 fences their writes. #1552 (heartbeat task abort on
external cancellation) shipped in #1778.

**Tests.**

- `chaos_tests::drain_hold::chaos_repro_1813_drain_releases_a_claim_that_never_started`
  uses the new chaos point `WORKER_DISPATCH_BEFORE_START`.
- `drain_release_tests::drain_joins_a_cooperative_activity_and_a_peer_retries_it`.
- `drain_release_tests::drain_keeps_the_claim_of_an_activity_that_ignores_the_cancel`.
- Unit tests for `drain_cancel_at` and the default.
