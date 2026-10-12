## Phase — Verify idle pool connections only (Ledger, pool recycle pings)

**What changed.** `pool::engine_manager(dsn)` builds the connection manager
with `RecyclingMethod::Fast`. `pool::with_engine_timeouts` adds a
`pre_recycle` hook that pings a connection only when it has been idle for
`pool::VERIFY_IDLE_AFTER` (1 second) or longer. A failed ping drops the
connection, and the pool takes the next one. `engine_pool`, the sharded pool
and the e2e bench build their pools this way.

**Why.** The default `RecyclingMethod::Verified` sends one `SELECT 1` round
trip on every checkout. On the e2e throughput scenario that was 9.401 pings
per claimed activity and 9.50% of all statements, with zero buffers. A busy
worker reuses each connection many times per second, so the ping told it
nothing.

**Trade-off.** A connection killed by the server less than a second after its
last use now comes back from the pool, and its first statement fails. The
engine already retries such a failure. A connection idle for a second or more
still gets the ping. An embedder that builds its own pool keeps its own
recycle method until it calls `engine_manager`.

**Evidence.** `autumn-harvest/scripts/pool_recycle_ping_perf_repro.sh`, 720
claimed activities. Baseline in `docs/perf-artifacts/pool-recycle-ping/`:
6,769 ping calls, 71,290 total calls. After: 0 ping calls, 65,071 total
calls. Tests: `an_idle_connection_that_the_server_killed_is_replaced_at_checkout`
and `a_recently_used_connection_is_handed_out_without_a_ping`.

No new `WorkflowEvent` variant, no new migration, no replay impact.
