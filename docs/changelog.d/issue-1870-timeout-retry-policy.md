## Fix — a timed-out activity attempt follows the retry policy (issue #1870)

Before this change, a `StartToClose` or `Heartbeat` timeout always appended
`ActivityTimedOut` and failed the task. It did not retry, even with attempts
left. A result write that failed on every repeat could thus fail a workflow
whose activity succeeded.

Now `timeout::enforce_activity_timeout` uses the retry rules of a retryable
`Err` in the worker. A timeout and an error share one attempt budget.

- A `StartToClose` or `Heartbeat` timeout with attempts left requeues the
  task. It uses the policy delay. With no policy, it uses a random delay from
  0 to 1 s (issue #1792). No event is appended. The heartbeat checkpoint
  stays, so the next attempt can resume.
- The last attempt appends `ActivityTimedOut` with its own timeout type and
  fails the activity call.
- A retry that cannot start before `schedule_to_close_at` appends
  `ActivityTimedOut { ScheduleToClose }` at once, as a worker retry does. A
  paused execution skips this check, because a pause stops that clock
  (issue #609).
- `ScheduleToStart` and `ScheduleToClose` timeouts stay terminal.
- A retry policy that does not parse makes the timeout terminal.

The timeout requeue keeps `crash_strikes` and the capability-miss counters.
A timeout does not prove that the handler ran to an end, so a crash loop still
reaches the poison-pill threshold.

Each retried timeout counts in `harvest.activity.retries` and feeds the
circuit breaker. It does not count in `harvest.activity.failed`.

The locked re-read now compares the row's claim, `attempt` and `started_at`,
with the scan. A requeue makes a new claim possible, so a sweeper with an old
scan could otherwise time out the next attempt. The check applies to
`StartToClose` and `Heartbeat` only. Tests replay a stale scan through the new
`#[doc(hidden)]` function `timeout::enforce_activity_timeout_for_task`.

Behavior change: an activity with no retry policy has `max_attempts = 3`. A
hung activity at the 10-minute `start_to_close` floor now runs up to three
attempts, so it can hold a worker slot for about 30 minutes. The old attempt
can still run when the next attempt starts. Use `ctx.idempotency_key()` for
each side effect. Set `RetryPolicy::fixed(1, ..)` to keep one attempt.

No new `WorkflowEvent` variant, no migration, and no `harvest_events` change.

Tests: unit tests for the retry decision and the claim guard, and the new
`activity_timeout_retry_tests` DB suite. The suite covers both timeout types,
the last attempt, the `schedule_to_close` limit, kept crash strikes and a
stale scan. A mutation run with the claim guard off fails the stale-scan test.
Three tests that pinned the old behavior now declare one attempt. The chaos
crash-restart test now requires `COMPLETED` for every workflow.
