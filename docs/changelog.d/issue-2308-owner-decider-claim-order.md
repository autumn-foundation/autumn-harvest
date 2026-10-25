## Engine — Owner decider takes workflow tasks in claim order (issue #2308)

The owner decider of issue #2255 took workflow tasks in wake order and by
`scheduled_at`. Under saturation, an older low-priority backlog could take
every workflow permit before a newer high-priority task. A new start also
competed equally with continuations.

The decider now sorts its ready set by priority, then by the due time with
the 30 s new-start handicap of issue #1824. A scan returns the first rows of
that order. A wake reads its row's key with one statement, only when the free
permits cannot cover every hint.

**Migration.** `20261024184859_harvest_owner_decider_order` adds the partial
index `idx_harvest_tq_owner_order` on `harvest_task_queue (decision_partition,
(new_start AND attempt = 0), priority DESC, scheduled_at) WHERE state =
'PENDING' AND task_type = 'workflow'`. The build is guarded, so an operator
can build it `CONCURRENTLY` first. No `WorkflowEvent` variant, no replay
impact.

**Behavior change.** A worker with `priority_aging_secs` runs no decider. It
keeps the owned claim of issue #2252, which applies the ageing boost. Owner
mode is off by default.
