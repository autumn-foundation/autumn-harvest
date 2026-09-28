## Fix — shard health names unhealthy assigned pollers (issue #1463)

The `no_live_worker` blocking reason said "no live worker assigned to this
shard polls queue(s)" when an assigned worker did poll the queue but was
stale, unhealthy, or draining. That text told operators to add coverage.

The gate now reports two cases. If no assigned worker polls the queue, the
reason says to start a worker or widen coverage. If an assigned worker polls
the queue but is not healthy and active, the reason says "no healthy active
worker" and to restore, restart, or reactivate it. The reason code is
unchanged. The `harvest_shard_undrained` runbook lists both branches.

Tests: four unit tests in `shard_health.rs` and the runbook guard in
`alert_pack_docs.rs`.
