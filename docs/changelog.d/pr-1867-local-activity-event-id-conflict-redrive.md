## Fix — re-drive a workflow task on a local activity event-id conflict (issue #1787)

**Local activity append conflict.** A local activity append that loses its
`event_id` to a concurrent append now re-drives the workflow task. Before, it
failed the run. The cursor snapshotted at task preparation is kept on purpose:
a handler that outlived its task timeout collides with the events a peer
committed, and the collision is what routes it to the claim fence. Re-reading
the cursor under the row lock would let that stale handler append past the
peer's events instead. Since #1787 (PR #1846), the poll path claims a race loser only
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
