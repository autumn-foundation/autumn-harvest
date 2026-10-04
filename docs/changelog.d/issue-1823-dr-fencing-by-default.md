## Engine — DR fencing on by default, admin writes fenced, batched claim fenced (issue #1823)

The cross-region DR generation fence (issue #954) was opt-in per process. A
process started without it could write to a demoted primary after a failover.
The fence did not cover admin tooling, and `claim_task_batched` had no fence
splice.

**Fence on by default where DR is configured.** `WorkerConfig::dr_fencing` is
now a `replication::DrFencing` mode: `Auto` (the default), `Enabled` or
`Disabled`. At startup a process probes each shard database for a DR marker:
a `harvest_shard_generation` row, a replication slot with the DR prefix, or a
subscription with the DR prefix. `Auto` fences when it finds one. A process
refuses to start when its configuration disagrees with the database:

- `Disabled` on a DR database;
- a fenced process that cannot name its shard, or names a shard other than
  the row in the database;
- a fenced process on a DR standby (a DR subscription, or recovery).

A database with no marker pays one probe per shard at startup (two catalog
reads) and runs the unchanged pre-#954 claim and persist paths. A worker
that cannot probe a shard holds it: the worker starts, but claims nothing
and appends nothing there until a background probe releases the shard or,
on finding a DR marker, stops the worker so it restarts and pins.
`HarvestRunner::start` retries a failed probe, then refuses to start.

**Admin writes go through the fence.**

- `HarvestRunner::start` pins every storage shard before the API serves, so
  API-only nodes fence too.
- Every mutating management API, Vantage, MCP and webhook route runs
  `assert_fence` for each pinned shard before its handler. A fenced node
  answers `503` and the handler does not run.
- `harvest partition enable|maintain|disable` and
  `harvest shard rebalance|rebalance-resume|reconcile-migrated-seals` take
  `--expect-generation <N>`. On a DR database the flag is required. The
  command refuses a shard at any other generation, which catches a stale DSN
  to a demoted primary.
- In-process partition maintenance checks the fence on each shard.
- `harvest dr fence`, `harvest dr promote`, `harvest migrate run` and the
  read-only commands stay exempt. `docs/cross-region-dr.md` says why.

**Every claim variant is fenced.** `claim_task_batched` now applies the same
fence CTE as `claim_task`, through one shared splice. The new
`claim_task_batched_on_shard` takes an explicit shard. A unit test reads
`queue.rs` and fails when a `pub async fn claim_task*` skips the fence.

**Automated failover stays out of scope.** `docs/cross-region-dr.md` § *Why
failover is not automatic* gives the reasons, and the rules for an external
HA manager.

### Upgrade notes

See `docs/upgrading/0.7.0.md` § 1.5. In short:

- `WorkerConfig::dr_fencing`, `DrConfig::fencing` and
  `WorkerConfigView::dr_fencing` changed type from `bool` to `DrFencing`.
  `with_dr_fencing(true)` still sets `Enabled`. `with_dr_fencing(false)` now
  sets `Disabled`, which refuses to start on a DR database. Remove it, or
  call `with_dr_fencing_mode(DrFencing::Auto)`.
- `GET /admin/config` reports `dr_fencing` as `"auto"`, `"enabled"` or
  `"disabled"`, not a boolean. It is the configured mode, not proof that the
  process fenced. The log line `pinned shard write-authority generation`
  shows that.
- On a DR database, a process with no shard identity needs exactly one
  `harvest_shard_generation` row. Otherwise it refuses to start.
- On a DR database, scripts that run `harvest partition` or
  `harvest shard rebalance` writes must pass `--expect-generation`.
- Restart the fleet after you first configure DR replication. A process
  decides at startup.
- New public API: `replication::{DrFencing, DrMarkers, pin_process_fence,
  probe_dr_markers, assert_admin_write_authority}`,
  `queue::{claim_task_batched_on_shard,
  claim_task_batched_candidates_query_fenced}` and
  `worker::dr_fence_targets`.
