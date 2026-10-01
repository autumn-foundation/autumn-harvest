## Refactor — One shared workflow-task backoff requeue (issue #1751)

Three functions in `autumn-harvest/src/queue.rs` re-pend a claimed workflow
task with a Postgres-clock backoff:
`requeue_workflow_task_nd_blocked`, `requeue_workflow_task_after_panic`, and
`requeue_workflow_task_for_quota_retry`. Each held a hand-copied `UPDATE`.
Four earlier fixes patched one copy and missed another (issues #1389, #1391,
#1589, #1402).

**The fix.** One private `UPDATE`, `requeue_workflow_task_with_backoff`, now
serves all three. `workflow_backoff_set` builds the shared `SET` columns:
the DB-clock `scheduled_at`, `wake_requested`, `activity_name`, and
`timer_fires_at`. The three public functions keep their names and
signatures. Each is now a one-line call that picks a sticky policy.

**Sticky policy.** ND-block and panic release sticky affinity. Quota retry
keeps it. A quota rejection is not a worker failure, so the pinned worker
stays valid. This keeps current behavior. The policy is a `StickyRelease`
changeset passed as `Option`, not a boolean mode flag. The doc comment on
`requeue_workflow_task_for_quota_retry` now states the choice.

**Tests.** Red phase: two no-DB tests for the new `workflow_backoff_sql`
seam failed to compile. Four DB tests in `workflow_backoff_requeue_tests.rs`
pinned the shared contract on the old code: re-pend and clears, the sticky
split, `NotFound` for an unknown task, and rejection of a `PENDING` task.
Green phase: all pass on the shared core. `retry_clock_skew_tests` still
pass.

No `WorkflowEvent` variant, no migration, no replay impact, no behavior
change.
