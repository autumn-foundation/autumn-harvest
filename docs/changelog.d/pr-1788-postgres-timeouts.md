## Phase 3.x — Bounded pool acquire and Postgres session timeouts (issue #1788)

A full pool or a stuck connection no longer stops claims and heartbeats.

**Pool timeouts.** `pool::engine_pool` and `pool::with_engine_timeouts` build a
pool with deadpool `wait` (30 s), `create` (10 s) and `recycle` (5 s) timeouts.
The workspace enables deadpool's `rt_tokio_1` feature, which these timeouts
need. `pool::acquire` bounds any pool, also a pool with no deadpool timeouts. A
timeout is the new typed error `HarvestError::PoolAcquireTimeout`.

**Bounded claim.** The single-shard claim, fleet registration and fleet status
writes now use the pool's `wait` bound, or 30 s when the pool has none. Before,
they waited without limit. This reverses the "single shard stays unbounded"
rule from issue #961 AC7. A claim that times out reports no work, and the poll
loop tries again.

**Session timeouts per role.** A `post_create` hook runs `SET` once on each
new connection. `DbRole::Hot` (claim and persist), `Scanner` and `Maintenance`
get growing `statement_timeout`, `lock_timeout` and
`idle_in_transaction_session_timeout` values. A zero value sends no `SET`.
`transaction_timeout` is off by default, because it needs PostgreSQL 17.
`ShardedDbPool::from_dsns` builds `Maintenance` pools.

**Heartbeat flush.** Each flush has its own bounded acquire. A failed flush
keeps its payload for the next tick.

**Metrics.** `harvest.db.pool_acquire_timeout{site}` and
`harvest.heartbeat.flush_failed{reason}`, with starter dashboard panels.

**Docs.** `docs/operations/postgres-timeouts.md` covers the role defaults,
`ALTER ROLE` for pools that Harvest does not build, and an `age(backend_xmin)`
alert.

No new `WorkflowEvent` variant. No migration. No `harvest_events` write.

**Tests.** Unit: `pool::tests` (setup SQL, role defaults, validation, bounded
`acquire` against a silent listener), `heartbeat::tests` (typed timeout, counted
retry), `worker::tests::a_single_shard_claim_is_bounded_and_counted`.
Integration (`pg_timeouts_tests`, real Postgres): a full pool fails a claim and
a heartbeat flush within the bound; `statement_timeout` cancels a `pg_sleep`
trigger in `store::append_events`; each role's connection reports its
configured timeouts.
