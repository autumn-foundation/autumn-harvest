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
  dispatch. No retry starts after `schedule_to_close`. Only the last attempt
  appends `ActivityTimedOut`. Schedule-to-start and schedule-to-close
  timeouts stay terminal. Use `max_attempts = 1` to keep a timeout terminal.
- An open breaker now defers work by default. `CircuitBreakerPolicy` has a
  new field, `open_mode`. `CircuitOpenMode::Defer` puts the task back to
  `PENDING` until the next probe. `CircuitOpenMode::FailFast` keeps the old
  `CircuitOpen` failure.
- A timeout feeds the breaker only when the attempt's handler started.
- A fleet that mixes 0.6 and 0.7 workers must keep timeouts terminal until
  the upgrade ends. See the 0.7.0 upgrade guide, §1.4.

**What shipped.**

- `timeout.rs`: `TimeoutReason::retries_per_policy`. The enforcer acts only
  on the scanned claim, identified by `worker_id`, `attempt` and
  `started_at`. It re-checks the heartbeat deadline under the row lock. A
  retry keeps `crash_strikes`, so poison-pill quarantine still counts
  crashes. A timeout retry counts in `harvest.activity.retries`.
- `worker.rs`: `timeout_retry_delay`, and the open-circuit deferral in
  `process_activity_task`. The delay is the time to the next probe, or the
  cooldown, clamped to 100 ms – 30 s, plus up to 25% jitter.
  `CircuitProbeGuard` releases a half-open probe on every early return, so
  an error cannot leave the breaker half-open for good.
- `queue.rs`: `mark_claim_handler_started`,
  `requeue_claimed_task_after_timeout` and
  `defer_claimed_task_for_open_circuit`. The deferral shares one fenced
  write with `defer_claimed_retry_for_budget`.
- `policy.rs`: `CircuitOpenMode` (in the prelude) and
  `CircuitBreakerPolicy::with_open_mode`. A serialized policy with no
  `open_mode` reads as `Defer`.
- New counter `harvest.activity.circuit.deferred` (`activity.name`), on the
  starter dashboard. `GET /admin/circuits` reports `open_mode`.
- Stall diagnosis says that an open breaker defers by default.
- Docs: ADR 0004; the circuit-breaker, triage, alert and containment
  runbooks; Chapter 7; the design doc; `architecture.md` §9; `telemetry.md`;
  the 0.7.0 upgrade guide (§1.2, §1.4 and §1.5); the 0.5.0 migration table; the
  SQLite crate docs, which now state that SQLite keeps terminal timeouts.

**Invariants.** Migration
`20261003202429_harvest_task_queue_handler_started_attempt` adds two nullable
columns to `harvest_task_queue`. The start transaction writes the claim's
`attempt` to `handler_started_attempt`, under the claim lock. The timeout
enforcer appends the timed-out claim's `started_at` to `timed_out_claims`.
No new
`WorkflowEvent` variant and no new `harvest_events` mutator. A retried
timeout appends no event, so replay is unaffected.

**Test evidence.** `activity_timeout_retry_tests` (Postgres):

- a start-to-close timeout with `max_attempts = 3` retries twice and then
  fails;
- a heartbeat timeout retries, keeps its checkpoint and keeps
  `crash_strikes`;
- the deadline stops a retry, except for a paused execution;
- schedule-to-start stays terminal;
- an unstarted timeout leaves the breaker closed, and a started one feeds
  it, on both the retry and the terminal path;
- a timeout of a `PENDING` task never feeds the breaker;
- an open breaker defers work, and the workflow completes after the breaker
  closes, both after `force-close` and after an organic half-open probe;
- `FailFast` fails the workflow with `CircuitOpen`.

Unit tests cover the retry delay, the defer delay, the policy default, its
serde form and the timeout-retry rule.

Also fixes a compile break on `trunk-dev`:
`activity_default_timeout_tests` lacked the `resident_workflows` field that
#1798 added.
