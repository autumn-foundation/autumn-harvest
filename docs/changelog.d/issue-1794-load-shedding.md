## Feature — automatic load shedding by backlog age (issue #1794)

A queue can now shed new workflow starts while its backlog is old. A shed
caller gets `429 Too Many Requests` with a `Retry-After` header. The manual
admission gate still answers `503`, so a caller can tell overload from an
operator halt.

Load shedding is opt-in per queue with `HarvestBuilder::load_shed`. Each
`LoadShedPolicy` has `trip_age`, `clear_age` and `retry_after`. The signal is
the oldest claimable `PENDING` task age, the same value as the
`harvest.queue.oldest_pending_age` gauge. A queue trips at
`age >= trip_age` and clears at `age <= clear_age`. Between the two it holds,
which is the hysteresis.

Design decisions:

- The state is in memory, per process, in `load_shed::LoadShedder`. The
  admission gate cache owns it. Each replica samples the same database.
- The start primitive checks the shedder after the manual gate, at the point
  where it decides to create a new execution with `GateMode::Check`. The gate
  sheds only a fresh create. Signals, updates, attaching starts, continuations
  and internal producers are exempt by construction.
- A throttled start can defer with `202` before it reaches the primitive. The
  start and batch routes therefore check the shedder before the throttle, with
  the same idempotent-retry bypass as the manual gate.
- An atomic batch whose only rejections are sheds answers `429` with
  `Retry-After`. A non-atomic batch reports each shed item as `rejected`.
- The gate fails open. A failed or slow sample changes no state. The gate
  ignores a state older than three sample intervals and then admits starts.
- `with_sample_interval` clamps the interval to the range from 1 second to
  1 hour (`MIN_SAMPLE_INTERVAL` and `MAX_SAMPLE_INTERVAL`). The sampler timers
  add the interval to the clock, so the bound keeps every deadline finite.
- A default deployment has no policy, so it runs no sampler and no SQL.

New surface: `HarvestError::LoadShed`, the `harvest.load_shed.active{queue}`
gauge, the `harvest.load_shed.rejected{queue}` counter, and the
`load_shed.trip` and `load_shed.clear` audit operations. `HarvestError` is not
`#[non_exhaustive]`, so a downstream exhaustive match needs a new arm. The
specification is `docs/operations/load-shedding.md`.

No new `WorkflowEvent` variant. No migration.

Test evidence: unit tests in `load_shed.rs` cover policy validation,
hysteresis, staleness, NaN ages and unconfigured queues.
`autumn-harvest-plugin/tests/load_shed_localpg.rs` builds a real backlog. It
asserts these behaviours:

- A new start gets `429` with `Retry-After` and writes no row.
- A signal and an attaching start pass.
- The gate holds inside the band and clears after the drain.
- The manual gate wins with `503`.
- Signal-with-start, throttled and batch starts are shed.
- A failed sample keeps the state.
