## Fix — the capability-miss release checks attempt (issue #1917)

`release_task_for_capability_miss` guarded on `worker_id` and
`crash_strikes`. A workflow cycle reaches this release after its handler
starts. The stuck-running requeue keeps `crash_strikes`, and the same worker
can win the row again. A stale cycle could then re-pend the live claim, and a
second worker could dispatch the task while the first cycle still ran.

The release now adds `AND attempt = $6` to all three phase arms. The arm
that lowers `attempt` checks the value before its `SET`, so it still matches
its own claim.

**API change.** `queue::release_task_for_capability_miss` takes a
`&queue::TaskClaim` in place of `task_id` and `worker_id`. The claim carries
the `attempt`. Build it with `TaskClaim::new(task.id, worker_id, task.attempt)`.

The TLA+ model `WorkflowTaskClaim` found the gap (issue #1819). Its fixed
config now checks the new guard. `WorkflowTaskClaimCapMissGap.cfg` became
`WorkflowTaskClaimCapMissPreFix.cfg`, which keeps the old guard as a
counter-example. `WorkflowTaskClaimCapMissFix.cfg` merged into
`WorkflowTaskClaim.cfg`. The #1819 fragment uses the old names.

Test evidence: the DB test
`a_capability_miss_release_from_a_stale_attempt_does_not_free_the_current_claim_1917`
covers all three phases. The unit test
`capability_miss_release_is_guarded_on_the_claim_attempt` pins the SQL.

No migration. No `WorkflowEvent` change.
