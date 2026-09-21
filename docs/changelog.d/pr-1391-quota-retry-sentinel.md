## Phase — Quota-retry backoff survives a stale sentinel (issue #1391)

A Codex review round on PR #1386 (issue #1227's quota-retry backoff), filed
as a follow-up because that PR's own 5-round review budget was already
spent, found a way to defeat the bounded backoff PR #1386 added.

**The defect.** `persist_mixed_suspension_batch` (`autumn-harvest/src/worker.rs`)
clears a previous cycle's `activity_name = 'mixed_signal_suspension'`
sentinel on its normal park path. That clear runs inside the same DB
transaction as the rest of the cycle. When the transaction instead fails
with `HarvestError::QuotaExceeded`, the whole transaction rolls back,
including that clear, and control falls through to
`recover_from_child_quota_exceeded` -> `queue::requeue_for_retry`, which
sets a future `scheduled_at` but never touched `activity_name`. A row that
entered the cycle already carrying a stale sentinel from an earlier,
unrelated timer-and-signal race (issue #476/#600) then sits `PENDING`,
future-scheduled, and still sentinel-marked -- exactly the shape
`primary_repend_workflow_task_query`'s wake-forward arm targets
(`state = 'PENDING' AND scheduled_at > $2 AND activity_name =
'mixed_signal_suspension'`). Any unrelated wake during the backoff window
then resets `scheduled_at` to now, and the bounded backoff never applies.

**The fix.** Added `queue::requeue_workflow_task_for_quota_retry`, which
mirrors `queue::requeue_for_retry` but restricts the update to
`task_type = 'workflow'` rows and clears `activity_name` in the same
statement -- the same pairing `requeue_workflow_task_nd_blocked` already
uses for the analogous ND-block backoff (issue #603). Routed
`recover_from_child_quota_exceeded` and `requeue_child_spawn_admission_error`
through the new function; both cover every `QuotaExceeded`/`ShardUnavailable`
child-spawn catch site in `worker.rs`, including the `persist_mixed_suspension_batch`
site above. `queue::requeue_for_retry` itself is unchanged and keeps serving
its one remaining caller, the plain activity-task retry path, where
`activity_name` holds the real activity name and must not be cleared.

**Tests, red -> green.** Confirmed the fix's regression test fails without
it (production code reverted in an isolated worktree, rebuilt, test
observed to fail) and passes with it restored. New test in
`autumn-harvest/tests/integration/quota_enforcement_tests.rs`,
`quota_retry_backoff_survives_stale_mixed_signal_suspension_sentinel`:
stamps a stale `mixed_signal_suspension` sentinel on a parent workflow
task's row, drives it through a real `QuotaExceeded` rejection via
`mixed_batch_quota_parent`'s activity/child race (the same fixture
`mixed_batch_child_spawn_honors_target_quota_parks_parent_then_succeeds`
from issue #1227 uses), confirms the backoff lands in the future, then
calls `queue::wake_workflow_task` directly to simulate an unrelated wake
and asserts `scheduled_at` is unaffected.

No `WorkflowEvent` variant, no data migration, no replay impact.
