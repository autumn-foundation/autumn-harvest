## Engine — Workflow-claim releases check `attempt` (issue #1806)

`queue::release_suspended_workflow_claim` and
`queue::release_terminal_workflow_claim` now match `attempt` as well as
`crash_strikes`. The first #1806 fix covered only the persist guard.

- The stuck-running requeue keeps `crash_strikes`. The same worker can then
  claim the row again. A release from the earlier claim matched the new claim
  and freed it to `PENDING`, while its handler still ran.
- The release `UPDATE` now adds `attempt = $4`.
- Both functions take a new `attempt` argument after `crash_strikes`. Pass
  `task.attempt`. This is a breaking change for these public functions.
- The capability-miss release is unchanged. It runs only on a worker that has
  no handler, so no stale owner write follows it.
- Test: `a_release_from_a_stale_attempt_does_not_free_the_current_claim_1806`
  claims, requeues as the stuck-running backstop does, claims again on the same
  worker, and runs the stale release. It failed before the fix. It also checks
  that the current claim still releases.

No migration, no schema change, no new `WorkflowEvent` variant.
