## Fix — A late heartbeat no longer quarantines live work (issue #1879)

**Bug.** The worker heartbeat slept one interval, then ran its tick. The
period was therefore the interval plus the tick latency. The stale window is
two intervals. A slow database made a live worker look dead. The reclaimer
then requeued its tasks with a crash strike. After three false strikes, a
healthy workflow went to the DLQ as `PoisonPill`.

**Fix, part 1: a fixed-rate heartbeat.** `spawn_worker_heartbeat` now uses
`tokio::time::interval_at` with `MissedTickBehavior::Delay`. Tick latency no
longer adds to the period. After a tick longer than one interval, the next
tick starts at once, with no burst. The first tick still comes one interval
after the start.

**Fix, part 2: a witnessed quarantine.** One sweep cannot tell a late
heartbeat from a dead worker. A strike is permanent, and a quarantine is
terminal. So the reclaimer loop now holds the last strike until its
`OrphanWitness` saw the same claim in every sweep for one more stale window.
Until then the row stays `RUNNING` and gets no strike. A worker that
heartbeats again keeps its task. The witness forgets all claims after a gap
between sweeps longer than one stale window, because a heartbeat in that gap
can go unseen. A DB pause therefore cannot confirm a death.

A requeue under the threshold does not wait, so crash recovery is not slower.
A true poison pill is quarantined one stale window later than before. It is
not dispatched again in that time.

**API.** New `poison_pill::reclaim_orphaned_tasks_witnessed`, `OrphanWitness`
and `OrphanClaim`. New field `ReclaimSummary::held`. It does not count in
`total()`. `reclaim_orphaned_tasks` keeps its signature and its first-sight
quarantine.

**Rejected.** A strike refund decrements `crash_strikes`. Claim fences use
that column to tell claims apart, so a refund can let a stale write match a
new claim. A wider `worker_stale_secs` slows crash recovery and changes three
other subsystems.

**Residual.** A false requeue under the threshold still counts a strike. It
cannot quarantine a task alone.

No migration. No new `WorkflowEvent` variant. `harvest_events` is not touched.

**Tests.** Paused-time unit tests for the heartbeat schedule. Unit tests for
the witness. DB tests: the last strike waits one stale window of witness; a
late worker that heartbeats again keeps its task; a requeue stays immediate.
The chaos latency test drops its 2 s heartbeat workaround and runs at 500 ms.
