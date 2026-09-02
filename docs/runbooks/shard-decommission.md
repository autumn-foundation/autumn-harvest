# Runbook: decommissioning a shard

**When to use this:** you want to remove a shard from a multi-shard Harvest
deployment entirely — because it is over-provisioned, because you are
consolidating, or because its hardware is going away — and its residents include
long-lived executions that will never reach a terminal state on their own.

**Before this existed** there was no path: `readable_shards` could not shrink
while any resident execution was alive, and a continue-as-new entity workflow is
alive forever. Shard rebalancing (issue #964) is what makes the drill possible.

Companion documents:

- `docs/sharding.md` § *Shard rebalancing* — the contract, the quiescence
  predicate, and what does and does not migrate.
- `docs/plans/2026-09-02-shard-rebalancing.md` — the design note, including the
  identity decision and the alternatives that were rejected.
- `docs/sharding.md` § *Adding a Shard* — the mirror image of this drill.

---

## The order is the safety argument

```
1. STOP NEW PLACEMENT   remove the shard from `writable_shards` (it stays readable)
2. DRAIN                rebalance its quiescent residents onto the successor
3. CONVERGE             repeat until zero residents remain; resolve the stragglers
4. FORWARD              declare the retired-shard forward, then drop it from `readable_shards`
5. RETIRE               keep or archive the database — never before step 4 is deployed fleet-wide
```

Each step is reversible until step 5. Steps 1–3 leave the shard fully
operational; a run migrated in step 2 is verifiably identical on the successor
before the source is sealed, and a wake arriving mid-migration aborts the
migration rather than being lost.

---

## 1. Stop new placement

Remove the shard from `writable_shards`, leaving it in `readable_shards`. This
is the existing drain state — the router already rejects pinned starts to a
drained shard with a `400`, and rendezvous hashing stops choosing it for new
work.

```rust
let router = ShardRouter::new(
    vec![ShardId::new(0), ShardId::new(1)],   // readable: BOTH, still
    vec![ShardId::new(1)],                    // writable: the successor only
    ShardId::new(1),
);
```

Deploy this to the whole fleet and confirm it with the config snapshot before
going further — a replica still holding the old map will keep placing new work
on the shard you are draining:

```bash
diff <(curl -s "$REPLICA_A/admin/config" | jq -S .shard_topology) \
     <(curl -s "$REPLICA_B/admin/config" | jq -S .shard_topology)
```

Also remap any `residency_map` key that targets the shard. A key pointing at a
readable-but-not-writable shard boots with a warning and rejects every start
under it.

## 2. Drain: rebalance the quiescent residents

**Dry-run first, always.** The dry run walks the same code path as the real run
up to the first write, so what it lists is exactly what would move.

```bash
harvest shard rebalance \
  --shard 0=postgres://.../harvest_shard0 \
  --shard 1=postgres://.../harvest_shard1 \
  --from 0 --to 1 --limit 200 --dry-run
```

Read the output before proceeding. Every skipped run names the reason it is not
quiescent, and those reasons are the work of step 3.

Then move a batch:

```bash
harvest shard rebalance \
  --shard 0=... --shard 1=... \
  --from 0 --to 1 --limit 200 --actor "$USER@$(hostname)"
```

Notes:

- **Both shards must be at the same migration level.** Run `harvest migrate run`
  against both first; a mismatch is refused up front rather than silently
  dropping a column.
- The command connects to the shard databases directly. Possession of both DSNs
  is the admin gate, and every attempt writes a `shard.rebalance.migrate` audit
  row on the source shard.
- `--limit` bounds one run. Repeat it; the drain is meant to be incremental.
- **Drain to a single successor.** See step 4 for why splitting a decommissioning
  shard across several targets costs you the ability to remove it.

If anything is interrupted — a killed process, a lost connection, a restart —
run the resume sweep before the next batch. It is idempotent:

```bash
harvest shard rebalance-resume --shard 0=... --shard 1=... --from 0
```

## 3. Converge: deal with what will not move

Re-run the dry run until it reports nothing left to migrate. What remains falls
into a few classes, each named by its blocker:

| Blocker | What to do |
|---|---|
| *a worker is currently running a workflow task* / *a workflow task is dispatchable right now* | Transient. Re-run; these clear on their own. |
| *an activity task is in flight* | Wait for it, or let it fail and retry. Activity-bearing runs are out of scope by design. |
| *a staged signal has not been folded into history* | Transient — the next decision cycle consumes it. |
| *a worker session is open* | Wait for the session to complete or expire. |
| *the execution has a parent* | **Migrate the root instead.** Only root executions migrate; a child moves with nothing. If the tree's root is on this shard, migrating it does not move the children — resolve these by letting the tree complete, or by resetting/terminating it deliberately. |
| *a non-terminal child workflow lives on this shard* | Same: wait for the children, or act on the tree. |
| *the run is blocked on a replay non-determinism* | Fix the divergence first (`docs/runbooks/nondeterminism-block.md`). |
| *the execution is not RUNNING* on a **paused** root | Resume it, migrate it, pause it again. Only `RUNNING` roots migrate. |
| *the execution holds / is queued for a durable mutex* | Wait for the lock to be released or granted. A mutex row is shard-local and keyed by the execution, so moving one side of it would strand the other. |
| *a dead-letter row is attributed to this execution* | Redrive or discard the DLQ entry first (`docs/runbooks/` DLQ guidance); it would otherwise be redriven against a sealed row. |

Confirm the shard is empty of live roots:

```sql
-- On the shard being decommissioned. PAUSED is in the list deliberately: a
-- paused root is not migratable, and a query that omitted it would report an
-- empty shard while stranding every paused run on it.
SELECT state, count(*) FROM harvest_workflow_executions
WHERE state IN ('RUNNING', 'PAUSED', 'MIGRATING') GROUP BY 1;
```

`MIGRATING` rows here would be debris from an interrupted inbound migration —
this shard is a source, not a target, so there should be none. If any appear,
run the resume sweep.

A non-empty `RUNNING` count means step 3 is not finished. **Do not proceed.**

## 4. Forward, then shrink `readable_shards`

Until now every migrated run has been reachable because the sealed source row on
shard 0 forwards to shard 1. Removing shard 0 from `readable_shards` takes that
pointer away, so declare the router-level forward **in the same deploy**:

```rust
let router = ShardRouter::new(
    vec![ShardId::new(1)],          // readable: the successor only
    vec![ShardId::new(1)],
    ShardId::new(1),
)
.with_shard_forwards([(ShardId::new(0), ShardId::new(1))]);   // ids minted on 0
```

Without the forward, an `ExecutionId` minted on shard 0 falls through to the
default shard and resolves to the *wrong* database, silently. The forward is
what keeps every pre-migration id — a parent's recorded `child_id`, a stored
handle, an external signal target, a webhook reference — resolving. **This is the
step that makes a decommission safe, and it is not optional.**

The router validates the map at construction: forwarding a shard that is still
readable, forwarding to a shard that is not readable, a self-forward, or a chain
of forwards all panic at boot rather than misroute at the first request.

> **Why a single successor.** A router forward is one shard → one shard. If you
> split a decommissioning shard's residents across several targets, no single
> declaration can resolve its ids, and the shard must stay in `readable_shards`
> as a **forwarding tombstone** — after the drain its content is a few kilobytes
> of sealed rows, so this is a cheap fallback, not a disaster. Draining to one
> successor is what makes full removal possible, which is why `harvest shard
> rebalance` takes a single `--to`.

Deploy fleet-wide and verify with the config diff from step 1 before step 5.

## 5. Retire the database

Only after step 4 is deployed everywhere:

- **Export the audit trail first.** Every migration wrote its
  `shard.rebalance.migrate` rows to *this* shard's `harvest_audit_log`, so
  retiring the database retires the record of the drill. Ship them off-box with
  the audit exporter (issue #953) before going further.
- **Do not drop the database until you are sure.** Take a final backup. The
  sealed `MIGRATED` rows are the audit record of where each run used to live.
- Remove the shard's pool from every node's configuration.
- Remove its DSN, backup jobs, monitoring and replication slots.

> **Never purge `MIGRATED` rows from a shard you are keeping readable.** They are
> not ordinary terminal rows: the forwarding pointer is what an id resolves
> through. A retention janitor that hard-deletes them breaks id resolution for
> every run that ever moved off that shard.

---

## Verifying the drill

The success bar, end to end:

1. `harvest shard rebalance --dry-run` reports nothing left to migrate on the
   retired shard.
2. Every pre-migration id still resolves — spot-check with a handful of
   `ExecutionId`s captured *before* the drain:
   ```bash
   harvest workflow get <execution-id-minted-on-the-retired-shard>
   ```
3. The retired shard's `harvest_shard_migrations` has no rows outside
   `DONE`/`ABORTED`.
4. No execution is claimable on two shards — by construction, since a source is
   sealed before its target is activated, but worth confirming on the successor
   that each migrated run has exactly one workflow task row.

## Rolling back

- **Before step 4** — restore the shard to `writable_shards`. Already-migrated
  runs stay migrated (they are verifiably identical on the successor); new work
  starts landing on the shard again.
- **After step 4** — remove the forward and restore the shard to
  `readable_shards`. The sealed source rows are intact, so origin-shard
  forwarding takes over from the router-level forward with no data movement.
- **After step 5** — restore the database from the backup taken in step 5, then
  roll back as above. This is why step 5 says take one.
