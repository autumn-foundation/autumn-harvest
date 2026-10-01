## Phase 3.x — Bounded pool acquire and Postgres session timeouts (issue #1788)

A full pool or a stuck connection no longer stops claims and heartbeats.

**Pool timeouts.** `pool::engine_pool` and `pool::with_engine_timeouts` build a
pool with deadpool `wait` (30 s), `create` (10 s) and `recycle` (5 s) timeouts.
The workspace enables deadpool's `rt_tokio_1` feature, which these timeouts
need. `pool::acquire` bounds any pool, also a pool with no deadpool timeouts. A
timeout is the new typed error `HarvestError::PoolAcquireTimeout`. Any other
pool failure, such as a refused connect, is `HarvestError::PoolAcquireFailed`.

**Bounded worker acquires.** Every worker acquire now uses the pool's `wait`
bound, or 30 s when the pool has none: the single-shard claim, fleet
registration and status writes, activity and workflow task persistence, the
cancellation observer and the DR generation pin. A zero `wait` still fails at
once when no slot is free, and its retries start 100 ms apart. Before, a pool
with no deadpool `wait` timeout made them wait without limit. This reverses the
"single shard stays unbounded" rule from issue #961 AC7. A claim that times out
reports no work, and the poll loop tries again.

**Session timeouts per role (opt-in).** A pool built with `engine_pool` or
`with_engine_timeouts` runs `SET` once on each new connection. The plugin's
`autumn-web` pools do not, so the operator guide gives the `ALTER ROLE` steps.
`DbRole::Hot` (claim and persist), `Scanner` and `Maintenance` get growing
`statement_timeout`, `lock_timeout` and `idle_in_transaction_session_timeout`
values. A zero value sends no `SET`. `transaction_timeout` is off by default,
because it needs PostgreSQL 17. A part of a millisecond rounds up, and `validate` rejects a value above
`i32::MAX` ms. `ShardedDbPool::from_dsns` builds `Maintenance` pools. The
partition drain sets `SET LOCAL statement_timeout = 0` for its census and its
move. A large DEFAULT census can need longer than a role limit, and a timeout
in the move discards a finished pass.

**Heartbeat flush.** Each flush has its own bounded acquire. A failed flush
keeps its payload and its receipt time for the next tick, so a retry cannot
make a stalled handler look alive. The flush writes through
`queue::record_heartbeat` with the `TaskClaim` fence from issue #1789, so a late
heartbeat cannot reach a newer attempt. A payload with no matching claim is
dropped. The
executed activity's result write uses `pool::acquire_with_retries` (10 bounded
tries). A try that fails early waits out its bound, so the tries also ride out a
short outage. A result write cancelled by a session `statement_timeout` or
`lock_timeout` runs again, up to 10 times (`pool::is_session_timeout`). With a
payload offloader there is one try, so no unreferenced blobs pile up. The
`CircuitOpen` failure write and the failure write for a retry policy that does
not parse use the same tries. A write that loses its connection, for example to
`transaction_timeout`, runs again on a new connection (`pool::is_connection_lost`). An activity task that still fails to get a
connection or hits a session timeout has its claim released, fenced on `attempt`
and `worker_id`, because an activity with no deadline would otherwise stay
`RUNNING`. A start that loses its connection is checked again first: if this
claim's `ActivityStarted` committed, the handler runs instead.

**Quarantine and session slots.** The workflow task timeout quarantine also
retries its acquire. Its write is fenced on the claim, so a late quarantine
cannot fail a run that a peer claimed in the meantime. A session acquire that
fails on a transient error keeps its slot until a read of the session row
settles it. When no read succeeds, a background task reads again. A new
acquire of that session on the same worker defers until the read ends.

**Local activity append conflict.** A local activity append that loses its
`event_id` to a concurrent append now re-drives the workflow task. Before, it
failed the run. Since #1787, a race loser that a freed permit admits appends
`ActivityStarted` while the winner's workflow task runs. The re-drive is the
one that the wake-event ingest uses (issue #779). This fixes
`race_resolved_by_activity_with_an_open_activity_loser_then_local_activity`,
which failed on every run on `trunk-dev`.

**Metrics.** `harvest.db.pool_acquire_timeout{site}` and
`harvest.heartbeat.flush_failed{reason}`, with starter dashboard panels.

**Docs.** `docs/operations/postgres-timeouts.md` covers the role defaults,
`ALTER ROLE` for pools that Harvest does not build, and an `age(backend_xmin)`
alert.

No new `WorkflowEvent` variant. No migration. No `harvest_events` write.

**Tests.** Unit: `pool::tests` (setup SQL, role defaults, validation, bounded
`acquire` against a silent listener), `heartbeat::tests` (typed timeout, counted
retry), `worker::tests::a_single_shard_claim_is_bounded_and_counted` and
`a_claim_on_a_pool_without_timeouts_uses_the_default_bound` (paused clock).
Integration (`pg_timeouts_tests`, real Postgres): a full pool fails a claim and
a heartbeat flush within the bound; `statement_timeout` cancels a `pg_sleep`
trigger in `store::append_events`; each role's connection reports its
configured timeouts.
