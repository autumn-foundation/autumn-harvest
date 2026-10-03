## Engine — Workflow-task ownership guard checks `attempt` (issue #1806)

`queue::claim_still_held_for_update` now matches `attempt` as well as
`crash_strikes`.

- The stuck-running requeue keeps `crash_strikes`. A same-worker re-claim
  passes the old guard, so a stale persist from the earlier attempt can
  commit.
- The guard now uses `claim_held`, the same predicate as activity writes. It
  adds `crash_strikes` and keeps `FOR UPDATE SKIP LOCKED`.
- The function takes a new `attempt` argument. `check_paused_and_park` and the
  four `persist_*_workflow_*` functions take it too. Pass `task.attempt`.
- The SQL const `claim_still_held_for_update_query` is removed.
- Tests: `persist_workflow_completion_rejects_a_stale_attempt_on_the_same_worker_1806`
  claims, requeues through `reclaim_orphaned_tasks`, re-claims on the same
  worker, and runs the stale persist. `the_guard_matches_only_the_current_attempt_1806`
  pins the guard.
