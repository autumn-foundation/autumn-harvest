## Deterministic suspension readiness (issue #1797)

The executor no longer uses the 100 ms `SUSPENSION_TIMEOUT` to decide that a workflow is suspended. A cycle now suspends when the handler is `Pending` and a Harvest future is parked. A Harvest future is parked when a buffered command holds an open result channel (`WorkflowCommand::awaits_result`), or when it holds a park token (a forever park or a false `await_condition`). The handler runs in `tokio::task::unconstrained`, so the coop budget cannot fake a `Pending`. The rule is in `docs/architecture.md`, Key Design Decision 10.

A handler that is `Pending` on a foreign future (for example a raw `tokio::time::sleep`) is polled again when that future wakes it. If it still waits after `executor::DEADLOCK_TIMEOUT` (2 s), the cycle returns the new `WorkflowOutcome::TaskFailed`. The worker discards the cycle's commands, appends no event and re-pends the task after 5 s. The run stays `RUNNING`, and the panic budget is not used. The SQLite backend returns the new `SqliteError::TaskFailed` and changes no state. `WorkflowTestEnv` and `WorkflowSimulator` stop with the error, and a replay report shows it as `WorkflowFailed`.

Effects:

- A single-step suspension no longer costs 100 ms. Median decision latency in the new unit test is well under 1 ms, down from 101 ms.
- A step that takes longer than 100 ms on a foreign future no longer causes a partial or zero-command suspension.
- `ctx.await_condition` is no longer a `const fn`.
- A Harvest future and a foreign future that are pending together suspend the cycle at once. The foreign future is dropped. Do not race the two kinds.

Hot code swap: constraint C9 in `docs/rnd/hot-code-swap.md` records the change. A `yield_now()` no longer causes a zero-command suspension.

No `WorkflowEvent` change and no migration.

Tests: `executor.rs` adds `suspension_outcome_does_not_depend_on_step_duration`, `foreign_await_past_the_deadlock_timeout_fails_the_task_not_the_run`, `harvest_parks_suspend_without_waiting_on_the_clock`, `single_step_suspension_decides_in_under_100_ms` and `yields_before_a_park_do_not_suspend_early`. The first four fail on the old timer. `panic_containment_tests.rs` adds `deadlocked_workflow_task_is_re_pended_and_the_retry_completes_the_run` against Postgres.
