# Postgres timeouts (operator guide)

Issue #1788. Applies to every Harvest deployment on Postgres.

## Why

PostgreSQL sets no statement, lock or idle-transaction limit by default. A pool
with no deadpool `wait` timeout makes every connection request wait without
limit.

Together these let one stuck connection stop work:

- A claim waits for a pool slot that never frees. The worker stops claiming.
- A heartbeat flush waits on the same pool. The heartbeat timeout then fails a
  healthy activity.
- A transaction left open holds `xmin`. Vacuum cannot remove dead rows, and the
  `SKIP LOCKED` queue scan slows down.

## Acquire bound

The worker never waits without limit for a connection. Each acquire uses the
pool's deadpool `wait` timeout as its bound. A pool with no `wait` timeout gets
30 s.

- An `autumn-web` pool sets `wait` from `database.connect_timeout_secs`, 5 s by
  default. The plugin's worker then uses 5 s.
- A multi-shard worker uses a shorter bound when it visits shards in turn:
  the poll interval, with a 5 s floor.

A timeout returns `HarvestError::PoolAcquireTimeout`. A claim that times out
reports no work, and the poll loop tries again. A failed fleet registration
arms the heartbeat retry. The write of an executed activity's result makes up
to 10 bounded tries, so a short pool incident does not drop the result.

`harvest.db.pool_acquire_timeout{site}` counts the timeouts that matter most.
`site` is `claim` or `heartbeat_flush`. Other sites log the error only.

## Pools that Harvest configures

`pool::engine_pool` and `pool::with_engine_timeouts` add two things to a pool.

First, deadpool timeouts:

| Timeout | Default | Bounds |
|---|---|---|
| `wait` | 30 s | The wait for a free slot. |
| `create` | 10 s | Opening a new connection. |
| `recycle` | 5 s | The check of an idle connection before reuse. |

Second, session timeouts. Each new connection runs `SET` for the timeouts of
its role, `autumn_harvest::pool::DbRole`:

| Role | Work | `statement_timeout` | `lock_timeout` | `idle_in_transaction_session_timeout` |
|---|---|---|---|---|
| `Hot` | Claim and persist | 30 s | 5 s | 5 min |
| `Scanner` | Background scanners | 5 min | 30 s | 5 min |
| `Maintenance` | Maintenance and operator tools | 30 min | 60 s | 10 min |

A pool serves every role when you give Harvest one pool. Use `Scanner` for that
pool, because its scanners need the longer limits.

A zero value sends no `SET`. The server or role default then applies. A part of
a millisecond rounds up to 1 ms.

`transaction_timeout` needs PostgreSQL 17 or later. It is off by default. Do not
set it on PostgreSQL 16 or earlier, because each new connection then fails.

`ShardedDbPool::from_dsns`, which `harvest shard rebalance` uses, builds
`Maintenance` pools.

The examples below run in a function that returns
`Result<_, Box<dyn std::error::Error>>`.

Use `engine_pool` for a new pool:

```rust
use autumn_harvest::pool::{DbRole, EngineDbTimeouts, engine_pool};

let pool = engine_pool(database_url, 16, DbRole::Scanner, &EngineDbTimeouts::default())?;
```

Use `with_engine_timeouts` when you build the manager yourself, for example
for TLS:

```rust
use autumn_harvest::pool::{DbRole, EngineDbTimeouts, with_engine_timeouts};
use diesel_async::pooled_connection::deadpool::Pool;

let builder = Pool::builder(manager).max_size(16);
let pool = with_engine_timeouts(builder, DbRole::Scanner, &EngineDbTimeouts::default())?
    .build()?;
```

`with_engine_timeouts` sets the Tokio runtime on the builder. deadpool needs
that runtime for its timeouts.

## Pools that Harvest does not configure

The plugin takes its pools from `autumn-web` in every mode. Those pools get the
acquire bound, but no session timeouts. Set the limits on the database role.
They then apply to every connection that the role opens:

```sql
ALTER ROLE harvest SET statement_timeout = '5min';
ALTER ROLE harvest SET lock_timeout = '30s';
ALTER ROLE harvest SET idle_in_transaction_session_timeout = '5min';
```

New connections get the values. Existing connections keep their old values
until they reconnect.

Run migrations as a different role, or clear the limits in the migration
session. A long migration step can need more than 5 minutes. A migration
runner can wait more than 30 s for the ledger lock while another runner works.

```sql
SET statement_timeout = 0;
SET lock_timeout = 0;
```

The partition drain (`partition::drain_default`) switches `statement_timeout`
off in its own transaction. A timeout there would discard a finished pass.

## Heartbeat flushes

Each activity heartbeat flush has its own bounded acquire. It does not wait
without limit on a full pool.

A failed flush keeps its payload and tries again on the next one-second tick.
`harvest.heartbeat.flush_failed{reason}` counts each failure. `reason` is
`acquire_timeout`, `acquire_error` or `write_error`.

Each write checks the claim: the row must be `RUNNING` under the same
`attempt` and `worker_id`. A task that finished, went back to the queue or has
a newer claim drops the payload. That case is not counted.

Alert when the rate stays above zero:

```promql
sum by (reason) (rate(harvest_heartbeat_flush_failed_total[5m])) > 0
```

A run of failures that is longer than an activity's `heartbeat_timeout` lets
that timeout fail the activity. Make the pool larger, or find the connection
that holds it.

## Alert on old transactions

`age(backend_xmin)` is the number of transactions since a backend took its
snapshot. A large value shows a session that holds back vacuum.

```sql
SELECT pid, usename, application_name, state,
       age(backend_xmin) AS xmin_age,
       now() - xact_start AS xact_duration,
       left(query, 120) AS query
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
ORDER BY age(backend_xmin) DESC
LIMIT 10;
```

Alert when the oldest value stays high. `postgres_exporter` exports
`pg_stat_activity_max_tx_duration`, the age in seconds of the oldest open
transaction. A rule can use it:

```promql
max(pg_stat_activity_max_tx_duration{datname="harvest"}) > 600
```

Start with a 10 minute threshold. Tune it to your longest legitimate
transaction. `idle_in_transaction_session_timeout` ends an idle transaction
before this alert fires. A long active statement still needs the alert.

## Related

- [`docs/telemetry.md`](../telemetry.md) lists the two counters.
- [`docs/architecture.md`](../architecture.md) describes `pool.rs` and
  `heartbeat.rs`.
