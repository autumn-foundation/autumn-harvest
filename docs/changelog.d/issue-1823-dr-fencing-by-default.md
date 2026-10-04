## Engine — DR fencing on by default, admin writes fenced, batched claim fenced (issue #1823)

The cross-region DR generation fence (issue #954) was opt-in per process. A
process started without it could write to a demoted primary after a failover.
Admin tooling was not fenced. `claim_task_batched` had no fence splice.

**Fence on by default where DR is configured.** `WorkerConfig::dr_fencing` is
now a `replication::DrFencing` mode: `Auto` (the default), `Enabled` or
`Disabled`. At startup a process probes each shard database for a DR marker:
a `harvest_shard_generation` row, a replication slot with the DR prefix, or a
subscription with the DR prefix. `Auto` fences when it finds one. `Disabled`
refuses to start when it finds one. A database with no marker pays one probe
query per shard at startup and runs the unchanged pre-#954 claim and persist
paths.

**Admin writes go through the fence.**

- The management API pins at startup in `HarvestRunner::start`, before any
  worker or request runs. Every mutating route then runs `assert_fence` for
  each pinned shard. A fenced node answers `503` and writes nothing.
- `harvest partition enable|maintain|disable` take `--expect-generation <N>`.
  On a DR database the flag is required. A shard at any other generation is
  refused, which catches a stale DSN to a demoted primary.
- `harvest dr fence`, `harvest dr promote`, `harvest migrate run` and the
  read-only commands stay exempt. `docs/cross-region-dr.md` says why.

**Every claim variant is fenced.** `claim_task_batched` now applies the same
fence CTE as `claim_task`, through one shared splice. The new
`claim_task_batched_on_shard` takes an explicit shard. A unit test reads
`queue.rs` and fails when a `pub async fn claim_task*` skips the fence.

**Automated failover stays out of scope.** `docs/cross-region-dr.md` § *Why
failover is not automatic* gives the reasons.

### Upgrade notes

- `WorkerConfig::dr_fencing` changed type from `bool` to `DrFencing`.
  `with_dr_fencing(true)` still sets `Enabled`. `with_dr_fencing(false)` now
  sets `Disabled`, which refuses to start on a DR database. Remove it to use
  `Auto`. `DrConfig::fencing` changed the same way.
- `GET /admin/config` reports `worker.dr_fencing` as `"auto"`, `"enabled"` or
  `"disabled"`, not a boolean.
- On a DR database, scripts that run `harvest partition` writes must pass
  `--expect-generation`.
- Restart the fleet after you first configure DR replication. A process
  decides at startup.
