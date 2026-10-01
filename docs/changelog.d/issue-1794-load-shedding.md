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
  admission gate cache owns it. Each replica samples the same database, so
  replicas converge. No migration.
- The shed check is in the start primitive at the point where the manual gate
  runs with `GateMode::Check`. So only a fresh create is shed. Signals,
  updates, attaching starts, continuations and internal producers are exempt
  by construction.
- The gate fails open. A failed sample changes no state. A state older than
  three sample intervals is ignored.
- A default deployment has no policy, so it runs no sampler and no SQL.

New surface: `HarvestError::LoadShed`, the `harvest.load_shed.active{queue}`
gauge, the `harvest.load_shed.rejected{queue}` counter, and the
`load_shed.trip` and `load_shed.clear` audit operations. The specification is
`docs/operations/load-shedding.md`.

No new `WorkflowEvent` variant. No migration.

Test evidence: unit tests in `load_shed.rs` cover policy validation,
hysteresis, staleness and unconfigured queues.
`autumn-harvest-plugin/tests/load_shed_localpg.rs` builds a real backlog. It
asserts `429` with `Retry-After` for a new start, a passing signal and a
passing attach, a hold inside the band, and a clear after the drain.
