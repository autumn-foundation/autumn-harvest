# ADR 0004: Activity timeout retries and the open circuit

## Status

Accepted (issue #1809).

## Context

- Every activity timeout failed the task terminally. The design doc said
  that start-to-close and heartbeat timeouts retry per policy. Temporal
  retries them.
- Every timeout of a `RUNNING` task fed the per-activity circuit breaker.
  A task that waited after its claim, and never ran its handler, also
  counted.
- An open breaker failed each later dispatch with a non-retryable
  `CircuitOpen`. Breaker state is per process.
- Together, these let overload become permanent failure. Overload causes
  timeouts, timeouts trip the breaker, and the breaker turns them into
  failures (#1785, #1787).

## Decision

### 1. Retry by timeout type

| Timeout | Retried | Reason |
|---|---|---|
| Start-to-close | Yes, per retry policy | One attempt hung. The next attempt can succeed. |
| Heartbeat | Yes, per retry policy | Same as start-to-close. |
| Schedule-to-start | No | A requeue goes back to the same starved queue. |
| Schedule-to-close | No | It is the deadline for all attempts together. |

A retried timeout follows the same rules as a retried handler failure:

- `next_retry_delay` computes the delay. The attempt cap, backoff, jitter
  and `non_retryable_errors` apply.
- A write fenced by the claim epoch (#1789) puts the row back to `PENDING`.
  The write appends no event and keeps the heartbeat details.
- The retry budget (#1793) gates the next dispatch, because its `attempt`
  is above 1.
- The timeout is not retried when the next attempt would start after
  `schedule_to_close`. A paused execution is the exception, because a pause
  stops that clock. The task then fails with its own timeout type.
- The last attempt appends `ActivityTimedOut` with its timeout type, as
  before. The workflow sees only that final outcome.

To keep a timeout terminal, set `max_attempts = 1` on the retry policy. A
timeout error has no typed error class, so `non_retryable_errors` matches its
full text, for example `timeout: StartToClose for charge_card`.

### 2. The breaker counts only attempts that started

A timeout feeds the breaker only when the handler of the timed-out attempt
started. The signal is the `harvest_task_queue.handler_started_attempt`
column. The transaction that appends `ActivityStarted` also writes the
claim's `attempt` there. Each claim increments `attempt`, so the column
equals `attempt` only after the current claim started its handler.

Timeouts of a `PENDING` task never feed the breaker. No handler ran.

A late result of a timed-out attempt does not move the breaker. The enforcer
already counted that attempt, and a late success must not clear the failure
window.

- The breaker keeps the claims that its process has in flight. Before its
  transaction, the enforcer marks such a claim. A result that arrives then
  is held until the transaction decides. If the transaction times the claim
  out, the held result is dropped. If not, it counts as usual.
- The worker reports to the breaker after its claim-fenced result write, and
  counts the outcome only when that write settled the attempt. A timeout
  enforced first, in this process or another one, leaves the write nothing
  to settle. The outcome is then dropped, and only a probe slot is released.
  The write is the one signal that every process shares.

### 3. An open breaker defers work by default

`CircuitBreakerPolicy` gets an `open_mode`:

- `CircuitOpenMode::Defer` (default). The worker puts the claimed task back
  to `PENDING`. The delay is the time until the next probe, clamped, plus
  jitter. The write lowers `attempt` again, keeps `error` and
  `crash_strikes`, and appends no event. The deferral uses no attempt. The
  `harvest.activity.circuit.deferred` counter counts each deferral.
- `CircuitOpenMode::FailFast`. The behaviour before this ADR: a
  non-retryable `CircuitOpen` failure.

In defer mode, a half-open probe that never reports would defer the work
forever. So the worker releases an admitted probe on every early return,
including an error.

## Consequences

- An activity must be idempotent. A timed-out attempt can still run when
  its retry starts. The claim-epoch fence (#1789) rejects the writes of the
  old attempt. The cancellation observer and the heartbeat flusher then stop
  it.
- A fleet that mixes 0.6 and 0.7 workers must keep timeouts terminal until
  every 0.6 worker is gone. A 0.6 worker has no claim-epoch fence, so its
  late writes can land on the retry.
- Final failure comes later. The worst case is `max_attempts` times
  `start_to_close`, plus backoff. Set `schedule_to_close`, together with an
  explicit `start_to_close`, to bound it.
- A timeout acts only on the claim that the scanner saw. If a later claim
  holds the row, the scanner leaves it alone.
- In defer mode, work waits while the breaker is open. With no
  `schedule_to_close`, a breaker that an operator forced open holds the
  work until `force-close`. Use `FailFast` when a workflow needs the fast
  failure, for example to run a Saga compensation.
- No new `WorkflowEvent` variant. Replay is unaffected. Histories written
  before this change replay unchanged.
- A timeout before the handler starts still uses an attempt. Only the
  breaker ignores it. The sweeper is not the claim owner, so it must not
  lower `attempt`: a lower value would let a later claim reuse a stale
  claim epoch. Since #1787 a claim waits for a free local permit, so this
  gap is short.
- A timeout retry keeps `crash_strikes`. A timeout does not prove that the
  attempt ended without a crash, so poison-pill quarantine (#367) still
  counts.
- Breaker state is per process. The breaker counts a timeout in the process
  that enforces it, not in the process that ran the attempt. That process
  drops the attempt's late outcome, so the attempt never counts twice. A
  shared breaker would count both in one place. It is out of scope.
- The SQLite backend keeps terminal timeouts and has no breaker feed. It
  is out of scope.

## Alternatives rejected

- **Count `ActivityStarted` events.** Paths that undo a claim before the
  handler starts, and orphan reclaims, make the count drift.
- **Compare the event timestamp with `started_at`.** The event timestamp
  comes from the host clock and `started_at` from the database clock
  (#1807).
- **Add `attempt` to `ActivityStarted`.** It changes the event schema for
  a signal that only the task row needs.
- **Do not claim while the breaker is open.** Breaker state is per process,
  so the claim query cannot see it.
