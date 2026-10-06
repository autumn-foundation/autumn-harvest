## Fix — A late heartbeat no longer quarantines live work (issue #1879)

**Bug.** The worker heartbeat slept one interval, then ran its tick. The
period was therefore the interval plus the tick latency. The stale window is
two intervals, rounded up to whole seconds. A slow database made a live
worker look dead. The reclaimer then requeued its tasks with a crash strike.
After three false strikes, a healthy workflow went to the DLQ as
`PoisonPill`.

**Fix, part 1: a faster, fixed-rate heartbeat.**

- `spawn_worker_heartbeat` uses `tokio::time::interval_at` with
  `MissedTickBehavior::Delay`. Tick latency no longer adds to the period.
  After a tick longer than one interval, the next tick starts at once, with
  no burst. The first tick still comes one interval after the start.
- The heartbeat `UPDATE` returns the row status (`heartbeat_worker_status`).
  The remote-drain check no longer needs a second round trip, so each tick
  is shorter.

**Fix, part 2: a confirmed last strike.** One sweep cannot tell a late
heartbeat from a dead worker. A strike is permanent, and a quarantine is
terminal. So the reclaimer loop holds the last strike until two conditions
are true:

- Two sweeps in a row saw the same claim (`OrphanWitness`). The claim key
  holds the task, worker, strikes, attempt and start time, so a new claim
  of the same task starts again from the first sight. One sweep right
  after a database pause sees every heartbeat as old. The next sweep sees the
  heartbeats of the live workers again.
- The worker wrote no heartbeat for two stale windows
  (`quarantine_confirm_secs`). The age compares `last_heartbeat_at` with
  `clock_timestamp()`, so both sides use the database clock. A skewed host
  clock cannot shorten the window, and a slow sweep does not reset it.
- A worker with no row has no heartbeat age. A live worker can lose its row
  for a short time, for example during a registration retry. So the
  reclaimer must see such a claim without a break for the full window.

Until then the row stays `RUNNING` and gets no strike. The stuck-running pass
skips a held row, because its requeue counts no strike. A worker that
heartbeats again keeps its task. A requeue under the threshold does not wait,
so crash recovery is not slower. A true poison pill is quarantined about one
stale window later than before. It is not dispatched again in that time.

**API.**

- New: `workers::heartbeat_worker_status`,
  `poison_pill::reclaim_orphaned_tasks_witnessed`, `OrphanWitness`,
  `OrphanClaim`, `quarantine_confirm_secs`.
- New pub field `ReclaimSummary::held`. `total()` does not count it. A struct
  literal of `ReclaimSummary` outside this repo must add the field.
- `reclaim_orphaned_tasks` and `heartbeat_worker` keep their signatures and
  their behavior.

**Rejected.**

- A strike refund decrements `crash_strikes`. Claim fences use that column
  to tell claims apart, so a refund can let a stale write match a new claim.
- A wider `worker_stale_secs` slows crash recovery and changes three other
  subsystems.
- A hold measured only by the reclaimer's own clock resets when a sweep is
  slow, so a poison pill can stay held while the database is slow.

**Residual.**

- A false requeue under the threshold still counts a strike. It cannot
  quarantine a task alone.
- A worker that restarts under the same `worker_id` within the hold makes its
  held claim look live again. This was already true within one stale window.
  The hold makes the window longer.

No migration. No new `WorkflowEvent` variant. `harvest_events` is not touched.

**Tests.**

- Paused-time tests drive the real heartbeat loop. They pin the fixed rate,
  the no-burst delay, the first tick, the zero-interval floor and the cancel.
- Unit tests for `OrphanWitness`, `quarantine_confirm_secs` and the
  database-clock sighting query.
- DB tests: the last strike waits for a second sweep; it waits while the
  worker is only late; a worker with no row waits the full window; a new
  attempt by the same worker restarts the witness; a late worker that heartbeats again keeps its task;
  the stuck pass skips a held row; a requeue stays immediate; the heartbeat
  write returns the status; the tick still detects a remote drain.
- A DB test of the spawned reclaimer loop: it holds a late worker's task, then
  quarantines it once the death is confirmed.
- The chaos latency test drops its 2 s heartbeat workaround and runs at
  500 ms.
