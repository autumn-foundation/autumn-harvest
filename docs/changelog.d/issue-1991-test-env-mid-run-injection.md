## Feature — mid-run signals, updates and queries in `WorkflowTestEnv` (issue #1991)

`WorkflowTestEnv` could queue signals only before the run started. A test
could not send a signal after the workflow reached a given point. It could
not run an update or a query against the running workflow. These tests had
to use the DB-backed integration path.

`WorkflowTestEnv::start(handler, input)` now returns a `WorkflowTestRun`.
A test drives it step by step:

- `run_until_blocked()` drives the run until the workflow blocks or the run
  ends. It returns `TestRunStatus::Blocked` or `TestRunStatus::Finished`.
- `signal(name, payload)` queues a signal. The next drive ingests it at
  task-prep, as the worker does.
- `update(name, input)` runs the validator. A rejection returns
  `UpdateRejected` and writes no event. An admitted update records
  `UpdateAdmitted` at the virtual time, runs the handler and records
  `UpdateCompleted` or `UpdateFailed`.
- `query(name, args)` runs a query handler and writes no event.
- `events()` returns the history so far. `finish()` drives the run to its
  end and returns the `TestRunOutcome`.

`WorkflowTestEnv::queries(..)` and `WorkflowTestEnv::updates(..)` register
declarative `#[query]` and `#[update]` handlers.

Design decisions:

- `run(handler, input)` is now `start(handler, input).finish()`. Both APIs
  share one engine loop, so the existing suite covers the new path.
- An update or a query replays the history into a fresh context. This is the
  same recipe the plugin query path uses. The handler then runs against the
  rebuilt state. The replay has metrics and durable logs off, so it does not
  count them twice.
- The update handler runs through `execute_admitted_update`, the API that
  completes an admitted update.
- After the run ends, `signal` and `update` return `WorkflowNotRunning`.
  `query` still serves the final state, as the query API does for a terminal
  execution (issue #612).
- Timers still fire when the workflow waits on them. A drive stops only on a
  wait with no timer. A held-timer mode is out of scope.

No migration. No new `WorkflowEvent` variant. No change to the worker.

Docs: `docs/getting-started/11-testing.md` shows the new calls. It also fixes
the `queue_signal` example, which called the builder on a `mut` binding.

Tests: `tests/integration/workflow_test_env_mid_run_tests.rs`. Each test
failed to compile before the change. They cover a signal after a block, the
update validator rejection, a failed handler, an unknown name, declarative
handlers, the virtual-time timestamp, a query before the first drive, and a
finished run. `replay_check` succeeds after a mid-run signal and a mid-run
update.
