# Postgres timeouts (operator guide)

Issue #1788. Applies to every Harvest deployment on Postgres.

## Why

PostgreSQL sets no statement, lock or idle-transaction limit by default. A pool
with no `wait` timeout makes every connection request wait without limit.

Together these let one stuck connection stop work:

- A claim waits for a pool slot that never frees. The worker stops claiming.
- A heartbeat flush waits on the same pool. The heartbeat timeout then fails a
  healthy activity.
- A transaction left open holds `xmin`. Vacuum cannot remove dead rows, and the
  `SKIP LOCKED` queue scan slows down.

## Pool timeouts

Harvest builds an engine pool with deadpool `wait`, `create` and `recycle`
timeouts.

| Timeout | Default | Bounds |
|---|---|---|
| `wait` | 30 s | The wait for a free slot. |
| `create` | 10 s | Opening a new connection. |
| `recycle` | 5 s | The check of an idle connection before reuse. |

The claim, the heartbeat flush, fleet registration and fleet status writes use
the pool's `wait` value as their bound. A pool with no `wait` timeout gets
30 s. A timeout returns `HarvestError::PoolAcquireTimeout`. The claim then reports
no work, and the poll loop tries again.

`harvest.db.pool_acquire_timeout{site}` counts each timeout. `site` is `claim` or
`heartbeat_flush`.

## Session timeouts per role

Each new engine connection runs `SET` for the timeouts of its role. Pick the
role with `autumn_harvest::pool::DbRole`.

| Role | Work | `statement_timeout` | `lock_timeout` | `idle_in_transaction_session_timeout` |
|---|---|---|---|---|
| `Hot` | Claim and persist | 30 s | 5 s | 5 min |
| `Scanner` | Background scanners | 5 min | 30 s | 5 min |
| `Maintenance` | Maintenance and operator tools | 30 min | 60 s | 10 min |

A pool serves every role when you give Harvest one pool. Use `Scanner` for that
pool, because its scanners need the longer limits. Partition and replication
maintenance set tighter `SET LOCAL` limits where they need them.

A zero value sends no `SET`. The server or role default then applies.

`transaction_timeout` needs PostgreSQL 17 or later. It is off by default. Do not
set it on PostgreSQL 16 or earlier, because each new connection then fails.

`ShardedDbPool::from_dsns`, which `harvest shard rebalance` uses, builds
`Maintenance` pools.

### Pools that you build

Use `engine_pool` for a new pool:

```rust
use autumn_harvest::pool::{DbRole, EngineDbTimeouts, engine_pool};

let pool = engine_pool(database_url, 16, DbRole::Scanner, &EngineDbTimeouts::default())?;
```

Use `with_engine_timeouts` when you build the manager yourself, for example
for TLS:

```rust
use autumn_harvest::pool::{DbRole, EngineDbTimeouts, with_engine_timeouts};

let builder = deadpool::managed::Pool::builder(manager).max_size(16);
let pool = with_engine_timeouts(builder, DbRole::Scanner, &EngineDbTimeouts::default())
    .build()?;
```

`with_engine_timeouts` sets the Tokio runtime on the builder. deadpool needs
that runtime for its timeouts.

### Pools that Harvest does not build

The `autumn-web` pool does not run the engine setup. Set the limits on the
database role. They then apply to every connection that the role opens:

```sql
ALTER ROLE harvest SET statement_timeout = '5min';
ALTER ROLE harvest SET lock_timeout = '30s';
ALTER ROLE harvest SET idle_in_transaction_session_timeout = '5min';
```

New connections get the values. Existing connections keep their old values
until they reconnect.

The claim and the heartbeat flush still get the 30 s acquire bound.

## Heartbeat flushes

Each activity heartbeat flush has its own bounded acquire. It does not wait
without limit on a full pool.

A failed flush keeps its payload and tries again on the next one-second tick.
`harvest.heartbeat.flush_failed{reason}` counts each failure. `reason` is
`acquire_timeout`, `acquire_error` or `write_error`.

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

Alert when the oldest value stays high. With `postgres_exporter`, a rule can
use its `pg_stat_activity` metrics:

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
