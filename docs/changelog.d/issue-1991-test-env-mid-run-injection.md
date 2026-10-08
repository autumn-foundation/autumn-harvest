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
- `events()` returns the history so far. `now()` returns the virtual time.
  `finish()` drives the run to its end and returns the `TestRunOutcome`.

`WorkflowTestEnv::queries(..)` and `WorkflowTestEnv::updates(..)` register
declarative `#[query]` and `#[update]` handlers. With a workflow name set,
only the handlers of that workflow are registered, as in the worker. The
crate root also exports `WorkflowTestRun` and `TestRunStatus`.

Design decisions:

- `run(handler, input)` is now `start(handler, input).finish().await`. Both APIs
  share one engine loop, so the existing suite covers the new path.
- An update or a query replays the history into a fresh context. This is the
  same recipe the plugin query path uses. The handler then runs against the
  rebuilt state. The replay has metrics and durable logs off, so it does not
  count them twice.
- The update handler runs through `execute_admitted_update`, the API that
  completes an admitted update. The engine does not run a completed handler
  again on replay. So a change that the handler makes to state captured by
  the workflow body is gone at the next drive. The guide states this.
- After the run ends, `signal` returns `WorkflowNotRunning`. `update`
  returns `UpdateRejected`, as the update API does for an execution that is
  not `RUNNING`. `query` still serves the final state, as the query API does
  for a terminal execution (issue #612).
- An update checks the declarative `arg_schema` (issue #610), then every
  validator that `validate_update` knows. A rejection writes no event.
- An update handler has a 5 second limit, as the default in-process query
  timeout. A timeout leaves the admitted update with no result. A panic
  records `UpdateFailed`.
- A drive with no new event and no pending signal returns `Blocked` at once.
  It does not run the frontier code again.
- A classic timer still fires when the workflow waits on it. A held-timer
  mode is out of scope.
- A `CancelRaceLosers` command with timers only is no longer counted as
  progress. A run that waits after a child-timeout win now reports
  `Blocked`. Before, `run()` stopped at the iteration cap for that shape.

No migration. No new `WorkflowEvent` variant. No change to the worker.

Docs: `docs/getting-started/11-testing.md` shows the new calls. It also fixes
the `queue_signal` example, which called the builder on a `mut` binding.

Tests: `tests/integration/workflow_test_env_mid_run_tests.rs`. Each test
failed to compile before the change. They cover a signal after a block and
an update validator rejection. They also cover a failed handler, an unknown
name and declarative handlers, with the workflow-name filter. Further tests
check the virtual time, a query before the first drive and a finished run.
Other tests cover the update `arg_schema`, a handler that never finishes, a
handler that panics, and a wait after a child-timeout win.
`replay_check` succeeds after a mid-run signal and a mid-run update.
