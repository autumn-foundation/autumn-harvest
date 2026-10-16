## Phase 3.x — Retry deadlock and serialization aborts; lock-order table (issue #1822)

At the wired sites, a deadlock victim no longer fails its workflow.

**Retry helper.** `tx_retry::run_with_conflict_retry` runs one top-level
transaction. Postgres can abort it with `40P01` (deadlock) or `40001`
(serialization failure). The helper then runs the whole transaction again. It
makes up to 5 attempts. The first retry sleeps 10 to 20 ms. Each later sleep
doubles, with a 500 ms cap. Any other error returns at once. Inside an open
transaction the helper runs once, because a savepoint retry keeps the outer
locks. The outermost caller owns the retry. `tx_retry::classify_conflict` reads
the English message text, because Diesel keeps no SQLSTATE. The module is
`#[doc(hidden)]`, with no stability guarantee.

**Wired sites.**

- `persist`: the inline external signal and cancel persist,
  `persist_external_signal_inline`. Two workflows that signal each other in one
  cycle lock their own row, then the row of the peer. Before this change the
  victim wrote `WorkflowFailed: database error: deadlock detected`.
- `workflow_task`: the workflow-task persist. Its closure records metrics
  before it commits, so the engine does not re-run it in place. A conflict now
  returns the error on every path. The dispatcher resets the task to `PENDING`,
  and replay derives the same decision. Before this change a terminal outcome
  with pending commands failed the workflow.
- `claim`: the three worker claim calls (`claim_task_of_kind_on_shard` twice,
  `claim_task_by_id_on_shard`).
- `scanner`: the debounce and throttle fire batches.

`fail_execution_on_error` now passes a conflict error through, like a
capability miss. A conflict that outlasts the retries of a wired site therefore
resets the task and does not fail the workflow. The dispatcher also releases an
activity claim after a conflict, as it does after a session timeout. Otherwise
an activity with no deadline would stay `RUNNING` under a live worker.

**Metrics.** `harvest.db.transaction_retry{site, reason}` counts each retry.
`harvest.db.transaction_retry_exhausted{site, reason}` counts a conflict that
remains after the last in-place retry. `reason` is `deadlock` or
`serialization_failure`. The two `MetricsRecorder` methods have no-op defaults.
The metrics-rs adapter bridges both. The starter dashboard pack has a new
panel, "Transaction conflict retries". `docs/operations/postgres-timeouts.md`
has a new on-call section.

**Lock-order table.** `docs/architecture.md` now lists the main lock-ordering
rules, directly after the `materialize_due_child_timeout_deadlines` ABBA
argument. That argument is unchanged. The table has 12 ordered paths and 5
known cycles. The `lock_order_docs` guard keeps the table next to the argument
and checks that the argument keeps its rationale. It also checks that every
cited pin test exists.

**Known residuals.** An inline cancel can evaluate completion triggers, and
the throttle fire batch runs admission. Their counters and logs can repeat
after a retry. The `cancel_running` and mutex cycle (row 16) and the re-run
cycle (row 17) still return an error from an API start. Unwired transactions
still fail on a conflict. This change adds no `WorkflowEvent` variant and no
migration.

**Tests.** `tx_conflict_retry_tests` (Linux, live Postgres):

- `two_persist_transactions_deadlock_and_both_commit` forces the mutual-signal
  deadlock with a test trigger. Both workflows complete, with one
  `persist`/`deadlock` retry and no repeated event. With the retry off, the
  victim ends `FAILED`.
- `a_deadlocked_terminal_persist_runs_the_cycle_again` forces a deadlock in a
  terminal persist. The workflow completes. Without the change it ends `FAILED`.
- `a_conflict_in_a_claim_is_retried_at_the_claim_site` and
  `a_conflict_in_a_debounce_fire_batch_is_retried_at_the_scanner_site` raise
  one synthetic `40P01` inside the claim and the fire batch.
- `a_conflict_error_does_not_fail_the_execution` covers the passthrough.
- Helper tests cover a two-connection deadlock, a `REPEATABLE READ` `40001`,
  bounded attempts, a non-conflict error, a nested call and a clean commit.

Unit tests cover the classifier and the backoff bounds. The suites
`lock_order_docs`, `metrics_rs_adapter` and `dashboard_pack_docs` gain matching
guards.
