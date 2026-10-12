## Phase X.Y — SQLite ingests every staged signal (issue #2020)

The SQLite backend no longer drops signals read with `drain_signals`,
`try_receive_signal` or a push handler. Each decision cycle now appends every
staged signal to history before replay, in arrival order. This mirrors the
Postgres task-preparation ingest.

- `worker::SignalIngest` plans the events before replay. The transaction that
  persists the cycle outcome writes them first. A discarded cycle
  (non-determinism, a contained panic under its budget) consumes no signal. A
  panic seal writes the signals that its replay saw.
- The #476 rule stays. Before the first staged signal of a name, the ingest
  fires each race deadline of that name that falls at or before the signal
  arrival. So a late signal cannot win. An unrelated due timer never goes
  ahead of a signal (#1071).
- `ingest_awaited_signal` and `store::peek_pending_signal` are removed.
  `store::pending_signals` reads every staged signal of a run.
- Behaviour change: a signal that the workflow never reads now reaches
  history. The strict `WorkflowReplayer` reports it as unconsumed, as for a
  Postgres history.

No migration. No new `WorkflowEvent` variant. No public API change.

Tests: `tests/integration/signal_ingest.rs` (drain loop, push handler with
`await_condition`, `try_receive_signal`, arrival order, both race outcomes).
Each history replays with `ReplaySucceeded` on the core `WorkflowReplayer`.
Two `reuse_policy` tests now seed the legacy undelivered row that their
cleanup path still handles.
