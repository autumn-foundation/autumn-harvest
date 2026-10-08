## Fix — workflow-pause post-claim re-check (issue #1640)

`apply_post_claim_rechecks` (`queue.rs`) is the shared, post-claim
authoritative re-check both `claim_task_on_shard` and
`claim_task_batched` call after a claim's own `UPDATE` commits its
snapshot. It already re-checks queue pause (`queue_pause`, issue #619)
and activity pause (`activity_pause`, issue #807), but not
workflow-execution pause.

**The gap.** The claim query's own candidate filter excludes a workflow
task whose owning execution is already `PAUSED`, but that filter is
evaluated against the claim statement's own snapshot. Under `READ
COMMITTED`, a `pause_workflow_execution` commit landing in the window
between that snapshot and `apply_post_claim_rechecks` running (a later,
separate statement) was invisible to both, so a worker could dispatch and
run a workflow task whose pause was already committed and acknowledged
to the operator — the exact class of bug the queue-pause and
activity-pause re-checks exist to close, left open for workflow-level
pause. Flagged by review on issue #1340's batched claim path (PR #1618)
but confirmed pre-existing and shared by the already-shipped single-row
claim path.

**The fix.** New `execution::release_claim_if_workflow_paused`, wired
into `apply_post_claim_rechecks` as a third re-check gated on
`task_type == "workflow"` and a non-null `workflow_exec_id`, so an
activity claim pays no extra round trip. The release query restores
`attempt` (a hold consumes no retry budget) and is scoped to
`task_type = 'workflow'` so it can never release an activity task that
happens to share the paused execution's `workflow_exec_id` — a
workflow pause holds new workflow dispatch only; in-flight and pending
activities are untouched, per `pause_workflow_execution`'s own doc
comment.

**No commit-order advisory-lock barrier.** Queue pause closes its
equivalent residual window with a shared advisory lock held through
commit (`queue_pause::try_lock_queue_for_claim`, issue #619). This fix
mirrors `activity_pause::release_claim_if_activity_paused` instead: a
fresh-statement re-check with no barrier, accepting a sub-millisecond
residual window bounded to at most one already-claimed task per racing
worker. Two reasons, not just precedent: the two-argument advisory
keyspace is reserved for `queue_pause` alone by a CI-enforced source
guard, and a new lock taken after the claim (the execution is not known
until a task is in hand) would invert `pause_workflow_execution`'s own
row-lock order, risking an ABBA deadlock the `try`-lock design in
`queue_pause` was built specifically to avoid.

**Tests.** New
`autumn-harvest/tests/integration/workflow_pause_claim_recheck_tests.rs`
(issue #1640):
- `a_claim_that_beat_the_workflow_pause_is_released_with_its_attempt_restored`
  and `an_ordinary_claim_is_not_released_when_the_workflow_is_not_paused`
  pin the release query's guards directly.
- `a_workflow_pause_committed_mid_claim_still_holds_the_task`
  reproduces the race deterministically: stall a claim mid-statement on
  its rate-limit debit (the one part of the claim CTE that can block),
  commit the pause while the claim is parked, then confirm the claim
  returns `None` and the task stays `PENDING` with `attempt` restored.
  Fails against the pre-fix code (RED), passes after the fix (GREEN).
- `an_activity_task_of_a_paused_workflow_is_not_held_by_this_recheck` is
  a scope guard proving the new re-check never holds an activity task
  merely because its owning workflow execution is paused.

**No new `WorkflowEvent` variant, no migration, no schema change, no
public API change to `claim_task`/`claim_task_by_id`.** The fix adds one
function and one gated call on the existing post-claim re-check path.
