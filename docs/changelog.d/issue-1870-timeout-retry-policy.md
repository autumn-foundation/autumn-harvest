## Fix — a timed-out activity attempt follows the retry policy (issue #1870)

Before this change, a `StartToClose` or `Heartbeat` timeout appended
`ActivityTimedOut` and failed the task on the first attempt. The sweeper did
not read the retry policy. One DB blip during a result write could thus fail a
workflow whose activity succeeded.

Now `timeout::enforce_activity_timeout` uses the same retry rules as a
retryable `Err` in the worker:

- A `StartToClose` or `Heartbeat` timeout with attempts left requeues the
  task. It uses the policy delay, or a jittered 1 s with no policy. No event
  is appended. The heartbeat checkpoint stays, so the next attempt resumes.
- The last attempt appends `ActivityTimedOut` and fails the activity call.
- A retry that cannot start before `schedule_to_close_at` is terminal at once.
  A paused execution skips this check, because a pause stops that clock
  (issue #609).
- `ScheduleToStart` and `ScheduleToClose` timeouts stay terminal.
- A retry policy that does not parse makes the timeout terminal.

Each retried timeout counts as an activity retry and feeds the circuit
breaker.

The locked re-read now compares the row's `attempt` with the scan. A requeue
makes a new attempt possible, so a sweeper with an old scan could otherwise
time out the next attempt.

Behaviour change: an activity with no retry policy has `max_attempts = 3`.
A hung activity at the 10-minute `start_to_close` floor now runs up to three
attempts. Set `RetryPolicy::fixed(1, ..)` to keep one attempt.

No new `WorkflowEvent` variant, no migration, and no `harvest_events` change.

Tests: unit tests for the retry decision and the attempt guard, and the new
`activity_timeout_retry_tests` DB suite. The suite covers both timeout types,
the last attempt, the `schedule_to_close` limit and a stale scan. A mutation
run with the attempt guard off fails the stale-scan test. Three tests that
pinned the old behaviour now declare one attempt. The chaos crash-restart test
now requires `COMPLETED` for every workflow.
