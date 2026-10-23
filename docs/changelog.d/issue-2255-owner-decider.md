## Engine — Stage 1b: workflow tasks leave the contended claim path (issue #2255)

Stage 1b of epic #2247. [ADR 0009](../adr/0009-decision-owner.md) decision 13
records the design. Owner mode stays off until an operator turns it on.
Legacy mode does not change.

**Migration.** `20261023015935_harvest_owner_decider` adds the partial index
`idx_harvest_tq_owner_scan` on `harvest_task_queue (scheduled_at) WHERE
state = 'PENDING' AND task_type = 'workflow'`, and the function
`harvest_decision_fence`. The index build is guarded, so an operator can
build it `CONCURRENTLY` first. No `WorkflowEvent` variant, no replay impact.

**Owner decider.** In owner mode each worker runs one owner decider per
owned database, beside its claim loops. The claim loops then claim
activities only. The decider learns of work from the owner wake of a
partition and from a scan of its partitions by due time. The scan runs at
each tenure change and after a full scan. It also runs at the next due
time, at most one poll interval apart. A take is the owned
by-execution claim, so every claim gate, the claim guard and the
post-claim rechecks still apply. Each take runs in its own future, bounded
by the workflow permits, and a scan never stalls a take. The task row
serializes the decisions of one execution. A take that finds nothing backs
its execution off, from one poll interval up to 30 s. A wake ends the
backoff. So a refused row cannot hold the head of the scan. The scan index
leads with the partition, so a scan reads a few rows of each owned
partition. With owner mode off, the decider sends no query.

**Signals.** The decider keeps execution ids only, as hints. It
acknowledges nothing. A signal is a `harvest_signals` row before
`send_signal` returns (restate#4566). A new owner scans each partition that
it acquires. So a signal acknowledged just before an epoch bump reaches
it, also when the old owner held its wake.

**Wakes.** The sender keeps a plain owner wake to the writer's own
partition. Only an evict wake to that partition is dropped. A claim-held
re-pend that is due now wakes the owner of its partition. So a fenced
owner's re-pend reaches the new owner at once. Where the decider runs, an
owner wake does not wake the claim loops.

**Fence.** With a DR pin, an owner-mode decision fences both rows in one
statement. The generation row comes first, as ADR 0009 decision 6 orders.
It waits with no bound, as before. The owner row waits at most 250 ms.
Inside the fenced scope, an append checks neither row again when the start
fence took the same generation row. The scope records that row. The signal
and timer ingest runs in the fenced scope, so it fences once.

**Metric.** `MetricsRecorder::record_owner_take_duration` times each take
as `harvest_db_query_duration{op="take", kind, shard}`. A take reads no
queue head, so it is not a claim. The default records nothing, so a custom
recorder sees no workflow take until it adds the method. The starter alert
watches `take`, and the claim-by-kind panel shows it.

**Evidence.** `owner_decider_tests` proves 3 activity claims, 0 workflow
claims and 4 takes per 3-activity workflow. It delivers a signal once when
the epoch bumps just after the acknowledgement. One case has no owner at
the acknowledgement. In the other case, the old owner holds the wake. A due
timer fires through the scan, and the scan reads only owned and unstamped
due rows. `a_dr_fenced_owner_decision_sends_one_fence_statement` proves one
fence statement and one generation read per decision. Three more DR tests
cover a bumped generation, a held generation row and a colocated shard.
[`performance-owner-decider.md`](../performance-owner-decider.md) records the
assay 0014 runs and the fence bench.
