## Phase 6.x — Small primitives: matching wait, durable promise, `AllowAll` (issue #1985)

Issue #1985 asked for a decision on four primitives that peer engines ship.
Three shipped. This change declines the counting semaphore and gives the
reason in `docs/comparison.md`. The design record is `DESIGN-1985.md`.

**No migration. No new `WorkflowEvent` variant. No new route.**

**Payload-matching signal wait.** `ctx.wait_for_signal_matching(name, pred)`
and the typed `ctx.receive_signal_matching::<T, _>(name, pred)` wait for the
first signal of a name whose payload satisfies the predicate.

- `HistoryMatcher::match_signal_where` generalises the signal scan. A
  same-name signal that fails the predicate stays in `pending_signals` for a
  later wait.
- Its event index goes to `predicate_rejected_signal_events`. The
  unconsumed-history check excuses it, so strict and canary replay stay
  clean. The `harvest.signal.unhandled` metric still counts it.
- The wait reuses `WaitForSignal`. A predicate wait marks its name as
  probed, so a resident (warm) workflow does not resume on a non-matching
  payload.
- The predicate runs with no matcher lock held. It can call back into the
  context, and a panic in it does not poison the lock.

**Durable promise.** `ctx.new_promise()` and `ctx.promise(key)` return a
`DurablePromise`. Its token is `<execution-id>/<key>`. Any caller settles it
once with `durable_promise::resolve` / `reject`, `ctx.resolve_promise` /
`ctx.reject_promise`, or the HTTP signal route.

- A promise is a signal named `harvest.promise:<key>`. The idempotency key is
  the same string, so the first settlement wins.
- The token is recorded in a `SideEffectRecorded` event. Replay under a new
  execution id, as in a reset fork, returns the same token.
- `wait` returns `HarvestResult<Result<T, PromiseRejected>>`. `wait_timeout`
  races a durable timer. `PromiseSettlement::decode` decodes a raw race payload.
- `signal::send_signal_idempotent` enforces the rules on every path. A
  `harvest.promise:` signal gets its name as its idempotency key, and a
  payload that is not a settlement is refused. No other signal can use a
  `harvest.promise:` key.
- `harvest-verify` classifies the six new `WorkflowContext` methods.

**`AllowAll` overlap.** `OverlapPolicy::AllowAll` (`allow_all`) starts a new
run on every firing. It ignores `max_active_runs` in the tick, the manual
DAG trigger and the backfill, as Temporal does.

- Each dispatch phase of a tick (buffered drain, fire) starts at most
  `ALLOW_ALL_MAX_STARTS_PER_TICK` (100) runs. The next tick resumes deferred
  catch-up slots.
- Throttles still apply. A throttle slower than the cadence lets its pending
  backlog grow.
- **Rollback.** An older binary reads `allow_all` as `skip`. An update
  through an older binary then stores `skip`. Set the policy again after the
  roll forward.
- `OverlapPolicy::VALID_VALUES` now feeds the API error messages. The
  create-schedule contract description named `allow_all` before it existed,
  and omitted `cancel_other` and `terminate_other`. This change corrects it.

**Tests.**

- Unit: `replay.rs` `match_signal_where_*`, `context.rs`
  `wait_for_signal_matching_*`, `resident.rs` predicate wait,
  `durable_promise.rs`, `policy.rs` and `scheduler.rs` `allow_all` cases.
- Replay and test env: `tests/integration/small_primitives_tests.rs`. A
  changed predicate reports drift. A promise token replays under a new
  execution id.
- DB: `tests/integration/small_primitives_db_tests.rs`. Two `AllowAll` runs
  overlap, complete and replay clean. The tick limit defers catch-up slots.
  A manual DAG trigger passes the cap. A matching wait ignores order 41. A
  promise settles once, rejects, buffers an early settlement, settles from
  another workflow, and replays clean. The settlement rules hold on the plain
  signal path.
