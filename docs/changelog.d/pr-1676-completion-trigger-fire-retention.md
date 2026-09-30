## Phase 3.51.1 — Completion-trigger fire retention (issue #1676)

`backup verify` proves a delivered completion-trigger fire from the target's
`harvest_execution_summaries` row. That row expires at `--summary-age`. The
fire row never expired, so an old fire fell back to a timestamp guess. The
guess can report `completion_trigger_fire_lost` or `_unproven` on a coherent
restore.

Retention now deletes a fire row at the summary horizon.
`retention::purge_expired_trigger_fires` runs in the summary GC pass. A fire
is never later than its target's completion, so the fire expires no later
than the target's summary. `backup verify` no longer sees it.

A fire stays in two cases. Its outbox row still exists, so the relay is not
done. Its source execution row still exists, so the fire row still stops a
second fire for that run. With no summary horizon, no fire row is deleted.
That case keeps the timestamp guess described in the runbook §4.2(d).

Migration `20260930020953_harvest_completion_trigger_fires_fired_at_index`
adds one index on `fired_at`. No `WorkflowEvent` variant, no replay impact,
no `harvest_events` write.

Tests: `retention_summary_tests.rs` covers the prune, the outbox guard, the
source-execution guard, and the no-horizon case.
