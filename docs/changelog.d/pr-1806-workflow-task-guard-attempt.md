## Fix — workflow-task claim guard checks `attempt` (issue #1806)

The workflow-task ownership guard checked `worker_id` and `crash_strikes`. It
did not check `attempt`. A stuck-task requeue (`requeue_stuck_task`) keeps
`crash_strikes`. The same worker can then claim the row again with an equal
strike count. A persist from the first claim then passed the guard. It could
complete, fail or park the run that the second claim now drives.

**What shipped.**

- `queue::claim_still_held_for_update` takes a `&TaskClaim`. It uses the
  `claim_held` predicate, so workflow and activity writes share one claim
  check. It still checks `crash_strikes` and still uses `FOR UPDATE SKIP
  LOCKED`.
- `queue::release_suspended_workflow_claim` and
  `queue::release_terminal_workflow_claim` take a `&TaskClaim`. Their `UPDATE`
  adds `attempt = $4`. Before, a stale ambiguous-claim release could free a
  later claim of the same worker.
- `claim_still_held_for_update_query` is removed. A string test cannot prove
  the guard. A database test does.
- `check_paused_and_park`, `persist_workflow_completion`,
  `persist_workflow_failure`, `persist_child_workflow_completion` and
  `persist_child_workflow_failure` take an `attempt` argument after
  `crash_strikes`. This is a breaking change for these `#[doc(hidden)]`
  functions.

The capability-miss release still keys on `crash_strikes`. It runs only on a
worker without the handler, so no stale owner write follows it. Its reasoning
is in `docs/architecture.md`, "Activity claim epoch".

No new `WorkflowEvent` variant, no migration, no schema change.

**Tests, red then green.** `terminal_write_ownership_tests` gains
`persist_from_a_requeued_claim_of_the_same_worker_is_rejected_1806`. It claims
a workflow task, requeues it as the stuck-task backstop does and claims it
again as the same worker. It then runs the first claim's completion. Before the
fix, the completion committed `WorkflowCompleted`. After the fix, it returns
the claim-ambiguous error and appends nothing. A second test covers the stale
release.
