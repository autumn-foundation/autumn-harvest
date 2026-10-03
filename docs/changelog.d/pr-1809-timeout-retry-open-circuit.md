## Phase — Activity timeout retries and a deferring circuit breaker (issue #1809)

Activity timeouts were terminal, but the design doc said that start-to-close
and heartbeat timeouts retry per policy. Every timeout of a `RUNNING` task
also fed the circuit breaker, even when the handler never started. An open
breaker then failed later work with a non-retryable `CircuitOpen`. Overload
could thus become permanent failure (#1785).
[ADR 0004](../adr/0004-activity-timeout-retry-and-open-circuit.md) records the
decision.

**Behavior changes.**

- A start-to-close or heartbeat timeout now retries per the retry policy.
  `next_retry_delay` computes the delay. The retry budget gates the next
  claim. No retry starts after `schedule_to_close`. Only the last attempt
  appends `ActivityTimedOut`. Schedule-to-start and schedule-to-close
  timeouts stay terminal. Use `max_attempts = 1` to keep a timeout terminal.
- An open breaker now defers work by default. `CircuitBreakerPolicy` has a
  new field, `open_mode`. `CircuitOpenMode::Defer` puts the task back to
  `PENDING` until the next probe. `CircuitOpenMode::FailFast` keeps the old
  `CircuitOpen` failure.
- A timeout feeds the breaker only when the attempt's handler started.

**What shipped.**

- `timeout.rs`: `TimeoutReason::retries_per_policy`. The enforcer requeues a
  retryable timeout with `queue::requeue_claimed_task_for_retry`, fenced by
  the scanned claim. A timeout retry counts in `harvest.activity.retries`.
- `worker.rs`: `timeout_retry_delay`, and the open-circuit deferral in
  `process_activity_task`. The delay is the time to the next probe, or the
  cooldown, clamped to 100 ms – 30 s, plus up to 25 % jitter.
- `queue.rs`: `mark_claim_handler_started` and
  `defer_claimed_task_for_open_circuit`. The deferral shares one fenced write
  with `defer_claimed_retry_for_budget`.
- `policy.rs`: `CircuitOpenMode` (in the prelude) and
  `CircuitBreakerPolicy::with_open_mode`. A serialized policy with no
  `open_mode` reads as `Defer`.
- Docs: ADR 0004, the circuit-breaker runbook, Chapter 7, the design doc,
  `architecture.md` §9 and the 0.7.0 upgrade guide (§1.3, §1.4).

**Invariants.** Migration
`20261003202429_harvest_task_queue_handler_started_attempt` adds one nullable
column, `harvest_task_queue.handler_started_attempt`. The start transaction
writes the claim's `attempt` there, under the claim lock. No new
`WorkflowEvent` variant and no new `harvest_events` mutator. A retried
timeout appends no event, so replay is unaffected. The SQLite backend keeps
terminal timeouts.

**Test evidence.** `activity_timeout_retry_tests` (Postgres): a start-to-close
timeout with `max_attempts = 3` retries twice and then fails; a heartbeat
timeout retries and keeps its checkpoint; the deadline stops a retry;
schedule-to-start stays terminal; an unstarted timeout leaves the breaker
closed; a started one trips it; a later claim does not inherit an earlier
start; an open breaker defers work and the workflow completes after
`force-close`; `FailFast` fails the workflow with `CircuitOpen`. Unit tests
cover the retry delay, the defer delay, the policy default and its serde
form.

Also fixes a compile break on `trunk-dev`:
`activity_default_timeout_tests` lacked the `resident_workflows` field that
#1798 added.
