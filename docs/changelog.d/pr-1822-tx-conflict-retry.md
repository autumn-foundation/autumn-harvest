## Phase 3.x — Retry deadlock and serialization aborts; lock-order table (issue #1822)

A deadlock victim no longer fails its workflow.

**Retry helper.** `tx_retry::run_with_conflict_retry` runs one top-level
transaction. Postgres can abort it with `40P01` (deadlock) or `40001`
(serialization failure). The helper then runs the whole transaction again. It
uses capped, jittered backoff, with 5 attempts from 20 ms up to 500 ms. Any
other error returns at once. Inside an open transaction the helper runs once,
because a savepoint retry keeps the outer locks. The outermost caller owns the
retry. `tx_retry::classify_conflict` reads the message text, because Diesel
keeps no SQLSTATE.

**Wired sites.**

- `persist`: the inline external signal and cancel persist,
  `persist_external_signal_inline`. Two workflows that signal each other in one
  cycle lock their own row, then the row of the peer. Before this change the
  victim wrote `WorkflowFailed: database error: deadlock detected`.
- `persist`: the workflow-task persist. Its closure records metrics before it
  commits, so the engine does not re-run it in place. A conflict now returns
  the error on every path. The dispatcher resets the task to `PENDING`, and
  replay derives the same decision. Before this change a terminal outcome with
  pending commands failed the workflow.
- `claim`: the three worker claim calls (`claim_task_of_kind_on_shard` twice,
  `claim_task_by_id_on_shard`).
- `scanner`: the debounce and throttle fire batches.

**Metric.** `harvest.db.transaction_retry{site, reason}` counts each retry.
`site` is `persist`, `claim` or `scanner`. `reason` is `deadlock` or
`serialization_failure`. `MetricsRecorder::record_db_transaction_retry` has a
no-op default. The metrics-rs adapter bridges it. The starter dashboard pack has
a new panel, "Transaction conflict retries".

**Lock-order table.** `docs/architecture.md` now lists every lock-ordering
rule, directly after the `materialize_due_child_timeout_deadlines` ABBA
argument. That argument is unchanged. The table has eight ordered paths and
three known cycles. The `lock_order_docs` guard keeps the table next to the
argument and checks that the argument keeps its rationale. It also checks that
every cited pin test exists.

**Known residuals.** An inline cancel can evaluate completion triggers. Their
counters can count twice after a retry. Throttle admission counters inside the
fire batch can also count twice. No `WorkflowEvent` variant and no migration.

**Tests.** `tx_conflict_retry_tests` (Linux, live Postgres):

- `two_persist_transactions_deadlock_and_both_commit` forces the mutual-signal
  deadlock with a test trigger. Both workflows complete, with one
  `persist`/`deadlock` retry and no repeated event. With the retry off, the
  victim ends `FAILED`.
- `a_deadlocked_terminal_persist_runs_the_cycle_again` forces a deadlock in a
  terminal persist. The workflow completes. Without the change it ends `FAILED`.
- Helper tests cover a two-connection deadlock, a `REPEATABLE READ` `40001`,
  bounded attempts, a non-conflict error, a nested call and a clean commit.

Unit tests cover the classifier and the backoff bounds. The suites
`lock_order_docs`, `metrics_rs_adapter` and `dashboard_pack_docs` gain matching
guards.
