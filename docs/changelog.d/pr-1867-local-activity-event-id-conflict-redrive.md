## Fix — re-drive a workflow task on a local activity event-id conflict (issue #1787)

**Local activity append conflict.** The local activity prefix transaction
now reads the next `event_id` under the execution row lock, as the other
persist paths do, instead of the cursor snapshotted at task preparation. A
local activity append that still loses its `event_id` to a concurrent append
(the completion or retry-failure writes after the handler ran) re-drives the
workflow task. Before, either case failed the run. Since #1787 (PR #1846), the poll path claims a race loser only
when the winner frees its permit, so the loser appends `ActivityStarted` while
the winner's workflow task runs. The local activity prefix append, at an
`event_id` precomputed from the cycle's history load, then hit the
`harvest_events (workflow_exec_id, event_id)` key. The re-drive is the one the
wake-event ingest uses (issue #779): the transaction rolled back, and the fresh
history load reads past the other event. The re-drive is fenced on the
handler's claim `(worker_id, attempt)`, so a handler that outlived its task
timeout cannot release a peer's claim.

No new `WorkflowEvent` variant or field. No migration. No `harvest_events`
write.

Tests: `mixed_suspension_tests::race_resolved_by_activity_with_an_open_activity_loser_then_local_activity`
failed on every `trunk-dev` run since #1846 and passes with the fix.
`activity_claim_epoch_tests` gains the holder re-drive and the stale-claim
no-op cases.
