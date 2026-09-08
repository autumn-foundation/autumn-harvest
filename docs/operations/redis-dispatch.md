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

**What it delivers, measured.**
[`docs/assays/0008-redis-dispatch-integrated-throughput.md`](../assays/0008-redis-dispatch-integrated-throughput.md)
runs a real four-worker pool against a real Postgres, with and without the
channel. Draining a 10,000-workflow backlog on the 4-core reference machine,
the channel sustained a mean **173.04 completed tasks/sec**. The same pool on
the Postgres claim path completed **zero** task rows in the same window. Read
the channel as a large multiplier at a deep backlog. Do not read it as 10,000
tasks/sec: that assay is a kill on that figure, and the integrated path misses
it by more than fifty times.

Leave it off when the backlog is shallow. A shallow backlog makes the scan
cheap, so the channel adds a network hop and an operational dependency for no
measured gain. The same assay measures that cost directly: paced at 86.52
workflows/sec against an emptying queue, dispatch-latency p99 was 426.96 ms
with the channel and 146.09 ms without it.

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
| `key_prefix` | `harvest` | Prefix for every key the channel owns. A process serving a single non-default shard appends `:s<shard>` to it. |
| `consumer_group` | `harvest_workers` | Redis Streams consumer group the workers join. |
| `visibility_timeout_ms` | `60000` | How long a delivered reference may stay unacked before another worker recovers it. |
| `poll_interval_ms` | `20` | Wait for one blocking read when the channel is idle. |
| `reconcile_interval_ms` | `1000` | Interval of the reconcile sweep over due `PENDING` rows. |

Configuration validation bounds every key. `key_prefix` must not be empty,
because every key the channel owns carries it, and an empty prefix collides
with unrelated keys in a shared Redis. `consumer_group` must not be empty,
because Redis rejects an empty group name. `poll_interval_ms` must be between
1 and 5000, because the worker checks shutdown between blocking reads.
`visibility_timeout_ms` must be at least 1000, because the timeout has to
outlast one Postgres claim. `reconcile_interval_ms` must be at least 1. The
`autumn-harvest-redis` crate repeats the prefix and group checks at connect.

Queue names must not contain `:`. The colon separates the parts of every key
the channel builds, as the key layout below shows. A queue named
`email:delayed` would therefore build the same key as the delayed set of a
queue named `email`. The channel rejects such a name on publish and on read.

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
build without that feature carries no channel implementation, so configuration
validation **rejects** the URL rather than ignoring it. The error names the
feature.

**A configured URL that cannot connect fails startup, in every mode.** The
process refuses to boot and names the endpoint. The Postgres fallback below
covers the *running* state only: it takes over when Redis goes away under a
started process. It does not cover boot. A process that started without
reaching its configured Redis would look healthy and publish nothing, so the
channel fails fast and visibly instead.

At startup a process that enables the channel logs one `INFO` line naming the
endpoint, the effective key prefix and the consumer group. The endpoint is logged in
credential-free form: any `user:password@` part of the URL is removed first.
Redaction fails closed. When the authority cannot be isolated — a string with
no `://`, or an unencoded `/` inside the password — the log and the error
print `<redacted>` in place of the whole URL.

### Transport security

The connection is plaintext by default. A `redis://` URL sends the password
in cleartext, and every reference travels in the clear. Use `rediss://` on any
network you do not control.

This release carries no TLS transport, so `RedisDispatch::connect` rejects a
`rediss://` URL with an error that says so. The `redis` client's TLS stack
depends on an unmaintained crate that the dependency ledger refuses. Issue
#1429 tracks TLS support. Until then, keep Redis on a private network or
behind a TLS tunnel that terminates on the host.

## Key layout

Every key carries the **effective** prefix. The effective prefix is the
configured `key_prefix`, plus `:s<shard>` when this process serves a single
non-default shard. A process that serves the default shard — every unsharded
deployment — uses the configured prefix unchanged. The startup `INFO` line
prints the effective prefix.

The suffix gives each shard its own key family. One process per shard is how a
sharded fleet meets the single-shard limit below, and without the suffix every
process in that fleet would read one stream. A worker for shard B would then
read shard A's reference, probe B's database, find no row and ack the
reference as absent, while A's row waited for A's reconcile sweep. The suffix
extends the configured prefix and never replaces it, so a prefix that already
namespaces an environment keeps that namespace.

An upgrade moves a non-default shard to a new key family. References that the
previous release published for that shard stay in the old stream, and no worker
reads them again. Nothing is lost: the rows are still `PENDING` in Postgres,
and the reconcile sweep republishes them into the new family within one
`reconcile_interval`. Delete the old keys at leisure with a prefix scan.

With the default prefix and a queue named `email`:

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

### Sizing

Size Redis from the count of pending rows, not from the workflow history. A
pending row costs roughly one marker key and one stream entry of about 200
bytes. Ten thousand pending rows therefore cost single-digit megabytes.
A reference carries no payload and no workflow state, so the figure does not
move with activity input size. Markers expire, and an acked entry is trimmed,
so a drained queue returns to near zero.

## Why it does not lose or duplicate work

Two mechanisms carry the whole argument.

**The reconcile sweep is the durability floor.** Every
`reconcile_interval_ms` a worker reads due `PENDING` rows for its queues in
`(priority DESC, scheduled_at ASC)` order, one page per sweep, and publishes
them. The sweep keeps a cursor per queue, so a page of rows that a claim gate
holds never hides the rows behind it. A lost reference, a dropped hint, a
crash between commit and publish, and a full Redis restart all converge
through this sweep. Redis persistence is therefore not required for
correctness. A publish with the same due time as the held reference is a
no-op, but only after the channel verifies the reference still exists. A
marker whose reference vanished is rewritten, and the reference is recreated.

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

The second row is the one the acceptance criteria name. Two process-kill tests
in `autumn-harvest-redis/tests/worker_dispatch_e2e.rs` assert it against a real
Postgres and a real Redis, and they reach the window by two different routes.

`crash_between_claim_commit_and_ack_neither_loses_nor_duplicates` kills the
child on the **workflow** task. It uses a channel wrapper that aborts the
process inside `ack`.

`crash_on_the_activity_claim_neither_loses_nor_duplicates` kills the child on
the **activity** task. It uses the `DISPATCH_AFTER_CLAIM_BEFORE_ACK` chaos
point, armed to fire on its second hit, which is the activity claim. See
[`docs/testing/chaos.md`](../testing/chaos.md).

The chaos point covers the activity case only. The workflow case keeps the
wrapper, so the two cases do not share one failure mode.

## Failure modes, and what you see

| Situation | Behaviour | What the operator sees |
|-----------|-----------|------------------------|
| Redis unreachable at startup | Startup fails | One error naming the endpoint in credential-free form |
| Redis becomes unreachable while running | The worker enters a Postgres-only mode for a cooldown, then probes the channel again. The cooldown starts at the poll interval, doubles per failed reference read and stops at 30 s. Only a successful reference read clears it, because `maintain` and `publish` run on a different connection | Throughput returns to the Postgres numbers; work continues |
| A malformed entry reaches a stream | The worker acks and deletes it | One warning naming the entry id |
| A reference names a task kind with no free permit on this worker | The reference goes back to the stream for a peer | No error; a peer with capacity claims the row |
| Redis returns and the streams are empty | The reconcile sweep refills them | A latency bump of at most one reconcile interval |
| A row is `PENDING` but a claim gate holds it | The reference is released with exponential backoff, capped | No error; the row waits for its gate |
| A reference names a row that is absent | Three short releases, then an ack | No error; this covers a publish that raced its own transaction |
| `[harvest.redis] url` set on a build without the `redis` feature | Startup fails at config validation | An error naming the `redis` cargo feature |
| `[harvest.redis] url` set on a runtime with more than one shard pool | Startup fails before the channel is installed | An error naming the shard-pool count and issue #1312; run one process per shard instead |
| `rediss://` URL | Startup fails at connect | An error stating that this release carries no TLS transport (issue #1429) |
| `key_prefix` or `consumer_group` empty | Startup fails at config validation | An error naming the empty key |
| A worker queue name is empty or holds a `:` | Startup fails before the channel is installed; `Worker::new` repeats the check | An error naming the queue and the rule |

The fallback is the important one, and its scope is exact. It covers the
**running** state: a started process that loses Redis keeps working on the
Postgres claim path, so availability with Redis down equals availability with
Redis absent. It does not cover **boot**: a configured URL that cannot connect
fails startup instead, in every mode.

## Limits in v1

- **Single shard per process.** A reference carries a task id and no
  connection, so a runtime that owns several shard pools cannot tell which pool
  holds the named row. Two places enforce the limit. `HarvestRunner::start`
  rejects a configured URL before it installs the channel, so a process with no
  worker is covered too. `Worker::new` repeats the check. Neither is config
  validation, which cannot see the resolved pool. The reference carries a shard
  slot for the follow-up work.

  A sharded fleet may still use Redis dispatch by running one process per
  shard. Each such process owns its own key family automatically, through the
  `:s<shard>` prefix suffix in [Key layout](#key-layout). A central API process
  that spans several shards is still rejected, because it publishes for shards
  it cannot separate. Issue #1429 tracks true multi-shard routing.
- **No Redis Cluster.** v1 targets one Redis instance. The keys carry no hash
  tags, so a Cluster deployment spreads the streams, the delayed sets and the
  markers of one queue across slots, and the Lua scripts that touch them
  together fail. Redis Sentinel and a single primary are the supported shapes.
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
- [`docs/assays/0008-redis-dispatch-integrated-throughput.md`](../assays/0008-redis-dispatch-integrated-throughput.md) — the integrated, deployment-shaped throughput measurement and its verdict
- [`docs/performance.md`](../performance.md) — the Postgres claim-path numbers
