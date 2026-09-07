# Redis dispatch (operator guide)

Issue #1312. Applies to `autumn-harvest-plugin` built with the `redis` cargo
feature, plus the `autumn-harvest-redis` crate.

## What it is

Redis dispatch is a **dispatch channel**, not a queue. Postgres keeps every
`harvest_task_queue` row and stays the source of truth.

When a row becomes claimable the engine publishes a small **reference** to a
per-queue Redis Stream. The reference carries the task id, the queue name and
the due time. It carries no payload and no workflow state.

A worker reads references from the stream, claims the named row in Postgres
with the **full existing claim predicate**, and then acks the reference. Every
claim gate still applies: queue pause, activity pause, concurrency cap, rate
limit, sticky affinity, session pin, build routing, capability match, DR fence,
execution pause, circuit breaker, schedule-to-close and due time. Every write
to workflow state and history stays in the Postgres transactions that exist
today.

The channel removes one cost, and only one: the backlog scan and sort that the
Postgres claim performs on every claim.

## When to use it

Turn it on when the queue is deep and the claim path is the bottleneck.
`docs/performance.md` publishes 640 claims/sec at a 1,000-row backlog on the
4-core reference machine, falling to 29/sec at a 10,000-row backlog. The fall
is the backlog scan.

Leave it off when the backlog is shallow. A shallow backlog makes the scan
cheap, so the channel adds a network hop and an operational dependency for no
measured gain.

Redis dispatch does not raise durability, and it is not a way to survive a
Postgres outage. Postgres remains required.

## Configuration

```toml
[harvest.redis]
url = "redis://cache:6379"
key_prefix = "harvest"
consumer_group = "harvest_workers"
visibility_timeout_ms = 60000
poll_interval_ms = 20
reconcile_interval_ms = 1000
```

| Key | Default | Meaning |
|-----|---------|---------|
| `url` | unset | Redis connection URL. Unset means Redis dispatch is off. |
| `key_prefix` | `harvest` | Prefix for every key the channel owns. |
| `consumer_group` | `harvest_workers` | Redis Streams consumer group the workers join. |
| `visibility_timeout_ms` | `60000` | How long a delivered reference may stay unacked before another worker recovers it. |
| `poll_interval_ms` | `20` | Wait for one blocking read when the channel is idle. |
| `reconcile_interval_ms` | `1000` | Interval of the reconcile sweep over due `PENDING` rows. |

Each key has an environment override:

```bash
AUTUMN_HARVEST_REDIS__URL=redis://cache:6379
AUTUMN_HARVEST_REDIS__KEY_PREFIX=harvest
AUTUMN_HARVEST_REDIS__CONSUMER_GROUP=harvest_workers
AUTUMN_HARVEST_REDIS__VISIBILITY_TIMEOUT_MS=60000
AUTUMN_HARVEST_REDIS__POLL_INTERVAL_MS=20
AUTUMN_HARVEST_REDIS__RECONCILE_INTERVAL_MS=1000
```

An empty `AUTUMN_HARVEST_REDIS__URL` means "off", which matches
`AUTUMN_HARVEST_DATABASE__URL`.

Setting `url` needs the `redis` cargo feature of `autumn-harvest-plugin`. A
build without that feature carries no channel implementation, so it **rejects**
the URL at startup rather than ignoring it. The error names the feature.

At startup a process that enables the channel logs one `INFO` line naming the
endpoint, the key prefix and the consumer group. The endpoint is logged in
credential-free form: any `user:password@` part of the URL is removed first.

## Key layout

Every key carries the configured prefix. With the default prefix and a queue
named `email`:

| Key | Kind | Holds |
|-----|------|-------|
| `harvest:dispatch:email` | stream | References that are due now |
| `harvest:dispatch:email:delayed` | sorted set | References parked until their due time, scored by that time |
| `harvest:dispatch:email:delayed:payloads` | hash | Payload of each parked reference, keyed by task id |
| `harvest:dispatch:marker:<task_id>` | string | Publish marker that makes a publish idempotent per task id |

`autumn-harvest-redis/src/naming.rs` is the single source of truth for the key
shape. The older `harvest:queue:*`, `harvest:scheduled:*` and `harvest:dlq:*`
keys belong to the standalone task-queue adapter, which is a separate
component and is not what an operator enables here.

Markers expire after ten minutes. A marker is a deduplication hint, never a
lock: an expired marker only allows a republish, which the channel then
dedupes again.

Every worker joins the one consumer group named by `consumer_group`, so each
reference is delivered to exactly one worker. A worker names itself as the
consumer, which is what lets a peer recover its references after a crash.

## Why it does not lose or duplicate work

Two mechanisms carry the whole argument.

**The reconcile sweep is the durability floor.** Every
`reconcile_interval_ms` a worker reads due `PENDING` rows for its queues in
`(priority DESC, scheduled_at ASC)` order, bounded by a batch size, and
publishes them. A lost reference, a dropped hint, a crash between commit and
publish, and a full Redis restart all converge through this sweep. Redis
persistence is therefore not required for correctness.

**The Postgres claim is still the only `PENDING -> RUNNING` writer.** A
reference grants nothing. Two workers that somehow both hold a reference for
one row still race on `FOR UPDATE SKIP LOCKED`, and exactly one wins.

### Crash matrix

| Crash point | Postgres | Redis | Recovery |
|-------------|----------|-------|----------|
| Before the claim commit | row `PENDING` | reference in the pending entries list | Recovery re-delivers the reference. The claim succeeds once. |
| After the claim commit, before the ack | row `RUNNING`, owned by the dead worker | reference in the pending entries list | Poison-pill reclaim re-pends the row. The reconcile sweep republishes it. The stale reference finds the row not claimable and is acked as a no-op. |
| After the completion commit | row terminal | no reference | Nothing to do. |
| After a publish, then Redis restarts | row `PENDING` | reference lost | The reconcile sweep republishes it. |

The second row is the one the acceptance criteria name. The process-kill test
`crash_between_claim_commit_and_ack_neither_loses_nor_duplicates` asserts it
against a real Postgres and a real Redis.

## Failure modes, and what you see

| Situation | Behaviour | What the operator sees |
|-----------|-----------|------------------------|
| Redis unreachable at startup | Startup fails | One error naming the endpoint in credential-free form |
| Redis becomes unreachable while running | The worker falls back to the Postgres claim path for that iteration | Throughput returns to the Postgres numbers; work continues |
| Redis returns and the streams are empty | The reconcile sweep refills them | A latency bump of at most one reconcile interval |
| A row is `PENDING` but a claim gate holds it | The reference is released with exponential backoff, capped | No error; the row waits for its gate |
| A reference names a row that is absent | Three short releases, then an ack | No error; this covers a publish that raced its own transaction |
| `[harvest.redis] url` set on a build without the `redis` feature | Startup fails at config validation | An error naming the `redis` cargo feature |

The fallback is the important one. Availability with Redis down equals
availability with Redis absent, which is today's Postgres-only behaviour.

## Limits in v1

- **Single shard only.** A sharded runtime rejects Redis dispatch at
  validation. The reference carries a shard slot for the follow-up work.
- **Priority is best effort.** A stream delivers in publish order. Only the
  reconcile sweep publishes in priority order.
- **Sticky affinity is best effort.** A non-pinned worker releases the
  reference with backoff. Affinity stays a cache hint, never a correctness
  rule.
- **No new event variant and no migration.** The channel adds nothing to
  `harvest_events` and nothing to the schema.

## How to turn it off

Unset `harvest.redis.url`, or set `AUTUMN_HARVEST_REDIS__URL` to the empty
string, then restart the process. Every worker returns to the Postgres claim
path immediately. No Postgres row changes, and nothing needs to drain first.

Leftover Redis keys are inert. Delete them at leisure with a prefix scan; do
not run `FLUSHALL` on a shared Redis.

To remove the dependency completely, rebuild `autumn-harvest-plugin` without
the `redis` cargo feature.

## See also

- [`docs/autumn-workflow-architecture.md`](../autumn-workflow-architecture.md) §9.1 — the design in context
- `docs/plans/2026-09-07-redis-dispatch-worker-integration.md` — the full plan, including the reverse brainstorm
- [`docs/assays/0001-redis-adapter-throughput-ceiling.md`](../assays/0001-redis-adapter-throughput-ceiling.md) — the standalone throughput measurement and its caveats
- [`docs/performance.md`](../performance.md) — the Postgres claim-path numbers
