## Deterministic suspension readiness (issue #1797)

The executor no longer uses the 100 ms `SUSPENSION_TIMEOUT` to decide that a workflow is suspended. A cycle now suspends when the handler is `Pending`, no wake fired during the poll, and a Harvest future is parked. A Harvest future is parked in two cases. A buffered command holds an open result channel (`WorkflowCommand::awaits_result`). Or the future holds a park token (a forever park or a false `await_condition`). A wake during the poll makes the cycle poll again first, so a combinator that yields early still dispatches all its children. The handler runs in `tokio::task::unconstrained`, so the coop budget cannot decide the command batch. The rule is in `docs/architecture.md`, Key Design Decision 10.

A handler that is `Pending` on a foreign future (for example a raw `tokio::time::sleep`) is polled again when that future wakes it. The first foreign wait starts a clock. If the cycle still waits after `executor::DEADLOCK_TIMEOUT` (2 s), it returns the new `WorkflowOutcome::TaskFailed`. The worker discards the cycle's commands and appends no event. It re-pends the task under the claim fence, after 5 s doubling to 300 s per consecutive deadlock. The run stays `RUNNING`. A deadlock resets the panic strike count. The SQLite backend returns the new `SqliteError::TaskFailed` and changes no state. `WorkflowTestEnv` and `WorkflowSimulator` stop with the error, and a replay report shows it as `WorkflowFailed`.

Effects:

- A single-step suspension no longer waits for a timer. The new unit test asserts a median decision latency under 100 ms. The old floor was the 100 ms timer itself.
- A step that takes longer than 100 ms on a foreign future no longer causes a partial or zero-command suspension.
- A Harvest future and a foreign future that are pending together suspend the cycle at once. The foreign future is dropped. Do not race the two kinds.

Breaking changes:

- `WorkflowOutcome` gains the `TaskFailed` variant, and `SqliteError` gains `TaskFailed`. An exhaustive `match` on either must add an arm.
- `WorkflowContext::await_condition` is no longer a `const fn`.

Upgrade note: a run recorded under the old timer may hold commands that its workflow emitted after a short foreign await beside a parked Harvest future. The new rule emits those commands one cycle later, so such a run can block for non-determinism. Drain or reset such runs before the upgrade.

Hot code swap: constraint C9 in `docs/rnd/hot-code-swap.md` records the change. A `yield_now()` no longer causes a zero-command suspension.

No `WorkflowEvent` change and no migration.

Follow-ups, not in this change: a metric for workflow task failures; the same claim fence for the panic requeue; the query-replay drivers still use their own rule.

Tests: `executor.rs` adds tests for step-duration independence, a parked Harvest future racing a foreign step, the deadlock timeout, every park kind, a wake during the poll, a dropped Harvest future, a panic after a foreign wait, the coop budget, command discard on deadlock, CPU time before the first foreign wait, and decision latency. `testing.rs` and `simulator.rs` cover their `TaskFailed` paths, and `worker.rs` covers the backoff. `panic_containment_tests.rs` adds `deadlocked_workflow_task_is_re_pended_and_the_retry_completes_the_run` against Postgres.
