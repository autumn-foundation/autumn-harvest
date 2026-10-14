## Phase — SQLite: keyed signals, signal by workflow id, signal-with-start (issue #2028)

On SQLite, `send_signal` took only an `ExecutionId`. A retried delivery
staged the signal twice. No call could start a run or signal it in one step.

**What shipped**

- `send_signal_idempotent(exec, name, payload, key)` stages a signal once per
  key. It returns `true` when it stages and `false` when the run already
  holds the key. The probe runs before the state check, so a retry after the
  run ends is `false`, as on Postgres.
- `send_signal_by_workflow_id(workflow_name, workflow_id, name, payload, key)`
  signals the `RUNNING` run of the key. It refuses a terminal run with
  `WorkflowNotRunning`. A continue-as-new successor keeps the key, so the
  call reaches it.
- `signal_with_start(SignalWithStart { .. })` starts a run and signals it, or
  signals the run that exists, in one transaction. It follows the Postgres
  matrix: a terminal prior gets a fresh run.
- The by-id and signal-with-start calls dedupe a key across every run of the
  `(workflow_name, workflow_id)` chain. A promise key dedupes per run only.
- New errors: `WorkflowIdNotFound` and `BlankWorkflowId`. New types:
  `SignalDelivery`, `SignalWithStart` and `SignalWithStartOutcome`.

**Migration.** Additive and run on open. `harvest_signals` gains
`idempotency_key` and the partial unique index `uq_harvest_signals_idem` on
`(exec_id, idempotency_key)`. Each old promise settlement takes its name as
its key.

**Behavior change.** A seal (cancel, terminate, failure, replace) now keeps
an undelivered signal that has a key, so its key still dedupes. Unkeyed rows
are deleted, as before. `SqliteError` has two new variants. An exhaustive
`match` on it must add arms.

**Invariants.** No new `WorkflowEvent` variant. `harvest_events` stays
append-only.

**Tests.** `tests/integration/signal_with_start.rs` covers each acceptance
criterion. An injected write failure proves that signal-with-start rolls back
the start and the prior's cancel. Unit tests cover the migration, the
promise back-fill and the seal rule.
