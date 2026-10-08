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

## Out of Scope for This Runbook

- **Worker poll loop HA**: workers already coordinate via `FOR UPDATE SKIP LOCKED` in `queue.rs`. This runbook covers the scheduler tick path only.
- **`drain_buffered_schedule_runs`**: the buffered-run drain path (for `BufferOne`/`BufferAll` overlap policies) has a lower-severity double-dispatch risk. In practice, `WorkflowIdReusePolicy::RejectDuplicate` on scheduled IDs prevents double execution. A dedicated claim guard for drain is tracked separately.
- **Cross-region active-active**: single-region multi-replica is the target topology. Cross-region deployments with separate Postgres instances should pin the scheduler to a single region.
