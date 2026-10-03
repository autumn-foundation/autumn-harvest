# Runbook: Running Harvest Behind a Load Balancer (HA Deployment)

**Issue**: #350 — Make scheduler ticks safe under multi-replica HA deployments

## Overview

Harvest is designed to be embedded in your Autumn application as `HarvestPlugin`. In production, most Autumn apps run **two or more replicas behind a load balancer** for high availability. This is the default deployment topology and is fully supported.

This runbook documents how Harvest handles multi-replica HA safely, what operators need to know, and how to diagnose contention.

---

## How Schedule Firing Works Under HA

Every replica runs its own scheduler tick loop (default: every 1 second). On each tick, the scheduler queries `harvest_schedules` for due rows and fires them by starting workflow executions.

### The Claim Protocol (issue #350)

Before firing any due schedule slot, a replica atomically claims it:

```sql
UPDATE harvest_schedules
SET fire_claim_token = gen_random_uuid(),
    fire_claimed_until = NOW() + INTERVAL '30 seconds'
WHERE id = $schedule_id
  AND (fire_claim_token IS NULL OR fire_claimed_until < NOW())
```

- If this `UPDATE` returns **1 row**: the replica won the claim and proceeds to fire.
- If this `UPDATE` returns **0 rows**: another replica already holds the claim. The replica skips this slot without error.

Postgres serialises these `UPDATE` statements so only one replica can claim a given slot. The two Postgres execution paths (serial vs. concurrent) produce identical observable outcomes: exactly one workflow execution per `(schedule_id, logical_date)` slot.

### Exactly-Once Contract

> **For any `(schedule_id, logical_date)` pair, exactly one `harvest_workflow_executions` row is created.**

This contract holds across all documented HA topologies:

| Topology | Contract |
|----------|---------|
| N replicas, one Postgres, single shard | ✅ Guaranteed by atomic claim UPDATE |
| N replicas, one Postgres, multiple shards | ✅ Per-shard claim, same guarantee per shard |
| N replicas with `WorkerConfig::shard_assignments` | ✅ Each shard's tick loop is independent; claim is per-shard-pool |
| Single replica (default for development) | ✅ Unchanged behaviour; no new latency |

This contract does **not** depend on `WorkflowIdReusePolicy`. A schedule with any reuse policy fires exactly once.

---

## Crash Recovery

**Q: What happens if the replica that claimed a slot crashes before firing?**

The claim token expires after **30 seconds** (`fire_claimed_until = NOW() + INTERVAL '30 seconds'`). On the next tick after expiry, any healthy peer re-claims the slot and fires it.

The 30-second window is the **crash recovery bound**: a crashed replica's un-fired schedule slot will be retried by a peer within 30 seconds.

After a successful fire, `fire_claim_token` and `fire_claimed_until` are reset to `NULL`, so the schedule is ready for the next logical slot.

**Q: Can the same slot fire twice after a crash?**

No. If the crashed replica successfully called `start_or_load_workflow_execution` before crashing (before it could advance `next_run_at`), the retry by the healthy peer will receive `AlreadyExists` (because scheduled workflow IDs are deterministic: `sched:{workflow_name}:{logical_date}`). The `AlreadyExists` response is treated as a safe duplicate, not an error.

---

## Observability: Verifying the Contract in Production

### Metric: `harvest.schedule.fire_attempts`

Every tick-loop attempt on a due schedule slot emits this counter:

| Label `outcome` | Meaning |
|-----------------|---------|
| `claimed` | This replica won the atomic claim and will fire. |
| `lost_race` | Another replica already holds a live claim; this replica skips without firing. |

Use this metric to:

1. **Verify exclusivity**: `sum(rate(harvest_schedule_fire_attempts_total{outcome="lost_race"}))` should be `> 0` when running 2+ replicas — it proves contention is detected and handled.
2. **Detect domination**: if one replica emits 100% of `claimed` and others emit only `lost_race`, check that all replicas share the same database and shard routing.
3. **Detect misconfiguration**: see alert below.

### Recommended Grafana Panel

```promql
# Claim success rate per replica
sum by (instance) (rate(harvest_schedule_fire_attempts_total{outcome="claimed"}[1m]))

# Lost-race rate (healthy HA signal; should track replica_count - 1)
sum by (instance) (rate(harvest_schedule_fire_attempts_total{outcome="lost_race"}[1m]))
```

### Alert: `harvest_schedule_ha_domination`

See `docs/alerts/starter-pack-v0.1.0.json` for the full alert definition.

**Trigger**: `lost_race / (lost_race + claimed) > 0.98` sustained for 5 minutes.

**What it means**: Nearly all fire attempts across the cluster are `lost_race`. Either one replica is incorrectly holding claims (e.g., its clock is slow), or only one replica is writing to the shared Postgres (topology misconfiguration).

**Triage steps**:
1. Verify all replicas share the same `DATABASE_URL` / pool configuration.
2. Check for clock skew > 30 s between replicas (`fire_claimed_until` uses `NOW()` from the Postgres server, not the replica clock — so clock skew between replicas is not a risk, but a misconfigured separate Postgres instance is).
3. Check that no replica has `shard_assignments` that exclude it from processing the affected schedule's shard.
4. Inspect `harvest_schedules.fire_claim_token` and `fire_claimed_until` directly for the problematic schedule row.

---

## Topology Reference

### Single Replica (Development / Staging)

```
App replica 1 ─── tick ─── harvest_schedules (Postgres)
```

No contention possible. All claims are immediately self-owned. Behaviour identical to pre-HA behaviour.

### Two Replicas (Typical Production HA)

```
App replica 1 ─┐
               ├─ tick ─── harvest_schedules (Postgres)
App replica 2 ─┘
```

Both replicas tick at the same interval. For most schedule rows, only one replica sees the row as due at any given tick (the other may have already advanced `next_run_at`). On simultaneous ticks, exactly one claims and fires; the other emits `lost_race`.

Expected steady-state: `lost_race / claimed ≈ 0` for fast schedules (< tick interval), `lost_race / claimed ≈ replica_count - 1` at the exact slot boundary.

### Multi-Shard

Each shard has its own connection pool. Claims are shard-local — `fire_claim_token` on shard A is independent of shard B. The contract holds per-shard.

### `WorkerConfig::shard_assignments`

Workers with explicit shard assignments only poll their assigned shards. The scheduler tick follows the same assignment: `tick_once_sharded` iterates over all shards in the `ShardedDbPool`. If a replica's pool only contains a subset of shards, it only claims and fires schedules on those shards. The contract holds per-shard.

---

## Schema Changes (Migration)

`20260530000000_harvest_schedule_ha_claim` adds two nullable columns to `harvest_schedules`:

| Column | Type | Default | Purpose |
|--------|------|---------|---------|
| `fire_claim_token` | `UUID NULL` | `NULL` | Token held by the claiming replica |
| `fire_claimed_until` | `TIMESTAMPTZ NULL` | `NULL` | Expiry of the current claim |

**Backward compatibility**: both columns default to `NULL`. Single-replica deployments and deployments that have not yet run the migration behave identically to before. The claim UPDATE always succeeds when `fire_claim_token IS NULL`.

**Migration is additive**: no destructive ALTERs, no required backfills.

---

## Background Scanners Under HA (issue #1795)

Each worker runs a set of per-shard background scanners. The heaviest is the
timeout checker. It runs task and workflow timeouts, SLA checks, the outbox
and delivery scanners, session cleanup, codec rotation and mutex lease
reclaim.

Before #1795, every replica ran every pass on every tick. Scan load grew with
fleet size. Adding workers to clear a backlog added database load in
proportion.

### One active timeout checker per shard

The replicas elect one timeout checker per shard with a lease row in
`harvest_scanner_leases`:

| Column | Meaning |
|--------|---------|
| `shard_id`, `scanner` | Primary key. One row per shard and scanner kind. A checker whose shard scope is not just its lease shard adds the scope to the key, for example `timeout:1,2`. Workers on one pool with different `shard_assignments` then each lead their own scope. |
| `holder` | The `worker_id` of the replica that runs the scanner now. |
| `lease_until` | The holder renews this on each tick. After it passes, any replica can take the row. |
| `epoch` | Counts changes of holder. A renewal by the same holder keeps it. |

On each tick, a replica takes or renews the lease with one atomic upsert.
Then:

- **The holder** runs the full pass.
- **A standby** skips the pass. It still refreshes its active codec key,
  because codec key retirement counts on every live process to do that once
  per tick.

The lease uses the database clock (`clock_timestamp()`). Clock skew between
replicas cannot give two replicas a live lease. The upsert reads the clock
after its row-lock wait, so a renewal never returns an expired lease.

The lease is a load control, not a safety fence. Two replicas can both run a
pass for a short time, for example when a slow pass outlives its lease. That
is safe: every sub-pass is already safe with concurrent runners.

### Failover

| Event | Takeover |
|-------|----------|
| Graceful stop (deploy, scale-in) | The holder expires its lease on exit. A standby leads on its next tick. |
| Crash, kill, or network partition | A standby leads within the lease TTL plus one tick (default about 10.6 s). |
| The holder's pass fails three times in a row | The holder gives up the lease and stands by for one TTL. Another replica takes over. |
| Lease query fails (table missing, grant missing, lock wait over 1 s) | Each replica runs the pass, as before #1795. The worker logs one warning each time the lease query starts to fail. |

During the takeover window, no replica runs the pass on that shard. Work
that falls due in the window runs late by up to the effective TTL plus one
tick.

### Bounded scans

The four task-timeout scans (heartbeat, start-to-close, schedule-to-start,
schedule-to-close) return at most one batch per reason per pass (default 500
rows). A sweep walks the live task rows (`PENDING` or `RUNNING`) in creation
order. Each refill reads one page of up to 64 batches of live rows, through
the partial index `idx_harvest_tq_live_created`, and queues the expired ones.
Each pass then loads one batch by primary key and checks it again. So the
work of a pass does not grow with the backlog, and one sweep reads each live
row once. The next sweep starts again with the oldest row.

A sweep reads only the rows created before it started, and queues only those
that had expired by then. A row created later, or that expires later, waits
for the next sweep. The index scan applies the creation bound itself, so a
refill never reads the newer rows. So new rows cannot stretch a sweep or push
an old row out of it.

On a large deployment, a sweep takes more than one pass. It needs about
(live rows) / (64 × batch size) passes. At the default batch of 500, that is
one pass per 32,000 live rows. A timeout can be enforced that much later.
If the index is missing, the scan still works, but each page can read many
terminal rows.

A row that matches two reasons gets the first one, in the order above, if
that reason's sweep can still claim it: the row is ahead of its cursor or in
its queue. Each lane has its own sweep clock, and keeps it until the last
queue of its sweep drains. If the earlier lane has already passed the row,
the later reason takes it. So the row does not wait for a whole sweep. A
queued row keeps its reason, also when an earlier reason starts to match
later. If its reason stops matching before it loads, it moves to the first
other reason that still matches. It goes first in that reason's next batch,
within that reason's limit.

A row that fails to enforce is tried again first in the next batch, for at
most three passes in a row. Retried and queued rows share the batch limit.
After its third try, the row waits for the next sweep, so bad rows cannot
block the rows behind them. A leader that fails three passes in a row gives
up its lease, and another replica tries. A replica that comes back from standby starts a new
sweep. A batch that fails to load stays queued.

### Jitter

By default, each sleep is the interval times a random factor in
`[0.8, 1.2]`. The mean stays at the interval (500 ms), so the default
enforcement latency does not change. Replica ticks stop lining up.

### Settings

Set with `WorkerConfig::with_scanner_config(ScannerConfig { .. })`:

| Field | Default | Meaning |
|-------|---------|---------|
| `elect` | `true` | `false` makes every replica run every pass, as before #1795. |
| `lease_ttl` | 10 s | Failover bound after a crash. Capped at 300 s, then raised to at least three times the longest sleep. |
| `jitter` | 0.2 | Random spread of each sleep, as a fraction of the interval. Clamped to `[0, 0.9]`. |
| `timeout_interval` | `None` | Mean time between timeout-checker ticks. `None` uses the worker poll interval (500 ms). At least 10 ms. With `elect`, at most 4 h. |
| `timeout_batch_size` | 500 | Most rows per timeout reason that one pass enforces. Kept within 1 and 100,000. |

`GET /admin/config` reports the configured values under `worker.scanner_*`
and `worker.timeout_scan_*`.

### Observability

`harvest.scanner.pass` counts each tick that gets a database connection, by
`scanner`, `shard` and `role`:

| `role` | Meaning |
|--------|---------|
| `leader` | This replica holds the lease and ran the pass. |
| `standby` | Another replica holds the lease. This replica skipped the pass. |
| `unelected` | Election is off. This replica ran the pass. |
| `fail_open` | The lease query failed. This replica ran the pass anyway. |

```promql
# Passes that ran, per shard. Expect about one tick rate, not N.
sum by (shard) (rate(harvest_scanner_pass_total{scanner="timeout",role!="standby"}[5m]))

# Which replica leads each shard.
sum by (instance, shard) (rate(harvest_scanner_pass_total{scanner="timeout",role="leader"}[5m])) > 0
```

`harvest.scanner.tick` still increments on every replica, standby included.
A standby loop is alive and ready to lead, so the liveness alerts do not
change.

A non-zero `fail_open` rate means the lease query fails on that replica. It
is not the same as `unelected`, which means an operator turned election off.
While the query fails, every replica that sees the fault runs the pass. Check
that the `20261001191830_harvest_scanner_leases` migration ran and that the
storage role can `SELECT`, `INSERT` and `UPDATE` `harvest_scanner_leases`.
Check also for a transaction that holds a lease row lock.

To see the current holders:

```sql
SELECT shard_id, scanner, holder, epoch, lease_until, lease_until > NOW() AS live
FROM harvest_scanner_leases
ORDER BY shard_id, scanner;
```

### Known limits

- Only the timeout checker uses the lease. The poison-pill reclaimer,
  session-slot reconciler, pause auto-resume, quota reconcile, audit export
  and the metric samplers still run on every replica.
- Only the holder's settings and registries apply to the pass. Give every
  replica that shares a database the same codecs, history ceiling and
  scanner settings. A holder whose pass keeps failing gives up the lease.
- Activity circuit breakers are per process. Only the holder sees
  out-of-band timeouts, so only its breakers count them. Before #1795, each
  replica saw a random share.

---

## Out of Scope for This Runbook

- **Worker poll loop HA**: workers already coordinate via `FOR UPDATE SKIP LOCKED` in `queue.rs`. This runbook covers the scheduler tick and the background scanners.
- **`drain_buffered_schedule_runs`**: the buffered-run drain path (for `BufferOne`/`BufferAll` overlap policies) has a lower-severity double-dispatch risk. In practice, `WorkflowIdReusePolicy::RejectDuplicate` on scheduled IDs prevents double execution. A dedicated claim guard for drain is tracked separately.
- **Cross-region active-active**: single-region multi-replica is the target topology. Cross-region deployments with separate Postgres instances should pin the scheduler to a single region.
