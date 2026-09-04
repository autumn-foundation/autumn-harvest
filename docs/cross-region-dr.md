# Cross-region disaster recovery

**Status: shipped (issue #954).** Per-shard asynchronous replication to a
standby region, a fencing mechanism with teeth, a measured RPO, and a failover
procedure whose order is the safety argument.

Failover is **operator-initiated**. There is no automatic promotion, no
active-active writing, and no zero-RPO mode. This document is the topology and
the design; [`docs/runbooks/cross-region-failover.md`](runbooks/cross-region-failover.md) is the procedure you run
during an incident.

---

## The problem this solves

Harvest's availability ceiling is one Postgres region per shard. "Use your
cloud's cross-region replica" is the obvious answer and it is *most* of the
answer — but on its own it is dangerously incomplete for an event-sourced
engine. After a failover, old-region workers that come back — or that were
never dead, only partitioned — can still claim tasks and append events against
their local, now-stale database. That forks a workflow's history, which is the
one thing an event-sourced engine can never tolerate: a fork is not a stale
read that heals, it is two divergent truths about what a workflow did.

So Harvest ships three things, and deliberately not a fourth:

| Ships | Does not ship |
| --- | --- |
| A **fence**: a per-shard write-authority epoch enforced in the claim and persist SQL | Replication. That is **stock Postgres**. |
| A **measured RPO**: `harvest.replication.lag_seconds{shard}`, plus a starter alert | Any sidecar, broker, Redis, or agent |
| **Verification tooling** and a runbook that reuses the restore checks | Automatic (unattended) failover |

No new infrastructure lives in core. The bytes move by stock Postgres logical
(or physical) replication; Harvest only makes the *engine* aware of who is
allowed to write.

---

## Topology

One standby per shard, in the standby region. A shard is a database, so each
shard replicates independently and fails over independently — which is a
feature (blast radius) and a hazard (skew); see [Multi-shard skew](#multi-shard-skew).

```
        region A (primary)                    region B (standby)
  ┌──────────────────────────┐          ┌──────────────────────────┐
  │ shard 0  ──── publication├─────────▶│ shard 0  ──── subscription│
  │ shard 1  ──── publication├─────────▶│ shard 1  ──── subscription│
  └──────────────────────────┘          └──────────────────────────┘
        workers pinned to                     no workers running
        generation N                          until after failover
```

### Logical or physical?

Both work. The fence and the RPO metric are indifferent to which you choose;
they read `pg_replication_slots`, which covers both.

| | Logical (`CREATE PUBLICATION` / `CREATE SUBSCRIPTION`) | Physical (streaming replica) |
| --- | --- | --- |
| Granularity | Per database — one shard per subscription | Whole cluster |
| Cross-version | Yes | No |
| Standby readable | Yes, and writable (it is an ordinary database) | Read-only until promoted |
| **Sequences replicated** | **No — see below** | Yes |
| DDL replicated | No — apply migrations to both | Yes |
| Promotion | `DROP SUBSCRIPTION` | `pg_ctl promote` |

### Logical replication does not replicate sequences

**If you choose logical, this is the step that will bite you.** `harvest_events.id`,
`harvest_workflow_logs.id`, and `harvest_mutex_waiters.id` are `BIGSERIAL`.
Logical replication copies the *rows*, including those id values, but it does
not advance the standby's sequences. A promoted logical standby therefore holds
a full copy of `harvest_events` while `harvest_events_id_seq` still sits where
it was when the subscription was created — and the new primary's very first
append dies on a duplicate primary key. The failure is immediate, total, and
mystifying if you have not seen it before.

The promotion step fixes it:

```bash
harvest dr promote --shard 0=postgres://harvest@standby-b/harvest_shard0
```

which calls `replication::advance_sequences_after_promotion`: every sequence in
the schema — including an embedder's own, which the same `FOR ALL TABLES`
publication replicates and which carry the identical hazard — is set to match
its data, and the list of what it set is printed for the incident log.

It is a **separate verb from `fence`** on purpose. Folding it in would let an
operator fence a shard, believe the promotion complete, and discover the
un-advanced sequence only when the first workflow tried to make progress.

Physical replicas replicate sequences and do not need this; running it there is
a harmless no-op, so run it either way rather than remembering which kind you
have.

Choose physical when one cluster hosts one shard and you want the simplest
promotion. Choose logical when a cluster hosts several shard databases you want
to fail over independently, or when you need to cross a major version.

### Setting it up (logical)

On the primary, per shard database:

```sql
CREATE PUBLICATION harvest_dr FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('harvest_dr_shard0', 'pgoutput');
```

On the standby, per shard database — apply Harvest's migrations **first**
(logical replication does not carry DDL), then:

```sql
CREATE SUBSCRIPTION harvest_dr_shard0
  CONNECTION 'host=primary.region-a port=5432 user=harvest dbname=harvest_shard0'
  PUBLICATION harvest_dr
  WITH (create_slot = false, slot_name = 'harvest_dr_shard0', copy_data = true);
```

> Creating the slot separately, with `create_slot = false`, is required only
> when publisher and subscriber share one Postgres instance (as the drill's
> two-database topology does): `CREATE SUBSCRIPTION` runs in a transaction and
> slot creation waits for older transactions to end, so it would wait for
> itself. Across two real instances you can let it create its own slot.

> **The partitioned `harvest_events` layout (#958) needs the partitioned layout
> on BOTH sides.** Two things break otherwise. `publish_via_partition_root`
> defaults to false, so a partitioned table's rows publish under their leaf
> partition names, which a standby built from the migrations has no tables for —
> the apply worker stops on the first event. And the partitioned layout drops the
> events foreign key, so reclamation happens by `DROP TABLE` on a partition,
> which is DDL and is never replicated; the subscriber's own cascade does not
> fire either, because apply runs with replica trigger behaviour. A flat standby
> therefore accumulates dangling events and `harvest backup verify` reports it
> **Incoherent**, which blocks the failover this publication exists for. Setting
> `publish_via_partition_root = true` fixes only the first. `harvest partition
> enable` refuses while any publication covers `harvest_events`; run the
> partitioned layout on the standby too and pass
> `--allow-incompatible-publications`.

> **The `harvest_dr` prefix in those slot names is load-bearing, not cosmetic.**
> Harvest identifies *its* replication by slot-name prefix
> (`replication_slot_prefix`, default `harvest_dr`). Without that filter every
> walsender for the shard's database would count as a DR standby — including an
> unrelated logical-decoding consumer such as a CDC pipeline — so a shard whose
> real cross-region subscriber had disconnected would report itself protected
> and `harvest_replication_down` would never fire. Name your slots with the
> prefix, or set the knob to whatever you did name them.
>
> A **physical** standby configured without `primary_slot_name` has no slot to
> match, so give it `application_name=harvest_dr_shard0` in its
> `primary_conninfo` — Harvest falls back to `application_name` for exactly
> that case.

The role Harvest connects as needs `pg_monitor` to read the replication views:

```sql
GRANT pg_monitor TO harvest;
```

Without it the RPO degrades to *unavailable* — logged, never fatal — and you
lose the number, not the engine.

### Turning the fence on

```rust
let config = WorkerConfig::default()
    .with_dr_fencing(true)
    .with_replication_sample_interval(Duration::from_secs(15));
```

It is **off by default and off costs nothing**: with it off the claim query is
the byte-for-byte pre-#954 statement, the persist path issues no extra
statement, and no DR sampler is spawned.

With it on, each worker at startup provisions and **pins** every assigned
shard's `harvest_shard_generation` epoch, before it registers in the fleet and
before its first poll. If the epoch cannot be read the worker **refuses to
start** rather than running unfenced.

---

## The fence

`harvest_shard_generation` holds one row per shard in that shard's own
database: a monotonic epoch for "who is allowed to write here".

```
 shard_id | generation |          fenced_at          | fenced_by |    fenced_reason
----------+------------+-----------------------------+-----------+---------------------
        0 |          3 | 2026-08-30 09:14:22.113+00  | oncall    | failover to region B
```

Two structural checks use the pinned value:

- **Claim gate.** `claim_task` cross-joins the generation row into its
  candidate CTE. A worker whose pinned epoch no longer matches selects zero
  candidates: it cannot claim work at all. There is no extra round trip — the
  check rides the statement that was already being issued — and the rows it did
  not claim are untouched: no `attempt` burned, no state change, so a worker in
  the region that *does* hold authority picks them up.
- **Persist assert.** Every append takes the generation row `FOR SHARE` first.
  `FOR SHARE` is the load-bearing detail, not defensive noise: the fence bump
  takes the same row exclusively, so it cannot commit while an in-flight
  persist holds it, and any persist that begins after it commits sees the new
  epoch and fails. That is a commit-order barrier, not a racy read.

Promoting a standby is therefore: bump the epoch on the new primary, and every
worker still pinned to the old one is *structurally* unable to claim or append.
It stops with `HarvestError::ShardFenced` and increments
`harvest.shard.fenced{shard}`.

### Invariants

- **No new `WorkflowEvent` variant.** Fencing writes no workflow history. A
  fenced attempt appends nothing — not a marker, not a rejection event. Replay
  is untouched, and a history recorded before a failover replays identically
  after it.
- **No change to any existing table.** Two additive tables, both empty until a
  worker with `dr_fencing` enabled provisions them.
- **The pin is never refreshed.** A worker reads the epoch once and holds it
  for its lifetime. It must **never** adopt a newer epoch it observes: adopting
  is precisely the split-brain the epoch exists to prevent, because a worker
  the promoted region just evicted would quietly rejoin the fleet. A fenced
  worker is recovered by **restarting** it against the region that now holds
  authority — never by re-pinning it in place.

### Why not a new event, a trigger, or a `REVOKE`

| Rejected | Why |
| --- | --- |
| A `WorkflowFenced` event variant | Fencing is a property of a *database*, not of a workflow's history. Recording it in the log would make replay depend on operational topology, and every replayer would have to learn a variant that means nothing to the workflow. |
| A `BEFORE INSERT` statement trigger reading a session GUC | Structural for every writer with no call-site threading — genuinely attractive — but invisible at the call site and untestable from Rust without a database. The issue asks for a check in the existing claim/persist SQL, and that is what is auditable. |
| `REVOKE INSERT` from the app role at the promoted primary | Coarse: the same role serves the *new* region's own workers, so revoking fences the fleet you are trying to start. |
| Worker leases with a TTL | The lease lives in the database that just failed over. |

---

## What fencing does not do

**Read this before relying on it.**

Fencing is a property of **one database**. It cannot stop a worker in a
partitioned old region from writing to that region's *own*, still-running
Postgres — nothing on the promoted primary can reach it. The generation bump is
not a network partition remedy and does not pretend to be one.

The fence bites at exactly the two moments that decide whether a history forks:

1. A surviving old-region worker **reconnects to the promoted primary** — a DSN
   flip, a DNS failover, a restart — and is rejected.
2. The old region is **re-seeded from the new primary** for fail-back. The
   bumped epoch arrives with the data, and every worker still pinned to the
   pre-failover epoch is rejected there too.

Therefore: **isolating the old primary's database is a mandatory operator
step, not an optional one.** Demote it, cut it off at the network, or take its
role's connections to zero — the runbook's step 1 does this. The epoch is the
engine-level backstop that makes a returning worker structurally harmless; it
is not a substitute for stopping the old primary from accepting writes.

Two smaller limits, stated plainly:

- **Fencing is opt-in per process.** A process that never pinned an epoch — an
  admin script, a migration job, a worker with `dr_fencing` off — is not
  fenced. That is deliberate (it is what makes the feature zero-cost when
  unused) and it means your *tooling* must respect the failover too.
- **A bump fences everyone.** Workers in the new region pinned to the old epoch
  are fenced exactly as old-region workers are. That is why the runbook's order
  is fence → promote → verify → **then** start workers, and why bumping a
  generation on a healthy shard is a fleet-wide outage recovered by restarting
  the fleet, never by bumping again.

---

## Measured RPO

`harvest.replication.lag_seconds{shard}` answers one question: **how much
acknowledged work would failing over right now lose?**

It is sourced from replication positions, via a watermark trail. The DR sampler
writes a `harvest_replication_heartbeat` row each tick — a wall-clock instant
stamped against `pg_current_wal_lsn()` — and the RPO is the age of the newest
watermark the slowest standby has actually confirmed
(`confirmed_flush_lsn`/`restart_lsn`, read from `pg_replication_slots` on the
primary, scoped to this shard's database).

### Why not just `pg_stat_replication.replay_lag`

Because it goes blind in the incident you need it for. `replay_lag` is computed
from the subscriber's reply messages, so a subscriber whose **apply worker is
stuck** stops replying and the column stays `NULL` or frozen while real data
loss accumulates. This was measured, not assumed: with a subscriber's apply
worker blocked, the byte backlog grew monotonically while `replay_lag` never
left `NULL`. The watermark trail is immune — it is computed on the primary from
a position the standby has confirmed.

`replay_lag` is still reported (`harvest.replication.lag_seconds` falls back to
it when no watermark has been confirmed), and it remains reliable for physical
replicas. Seeing a large watermark RPO next to a `NULL` `replay_lag` is the
signature of a stuck apply worker.

### Unknown is not zero

When the RPO cannot be determined — no standby connected, no slot, or the
standby further behind than the retained watermark trail — the lag series is
**absent**, never `0`. A dead standby reported as a perfect RPO is the most
dangerous number this feature could publish. Alert on
`harvest.replication.standbys{shard} == 0` for "replication is down"; alert on
the lag gauge only for "replication is slow".

The beat also keeps WAL moving on an idle primary, so an idle deployment
reports a live RPO instead of a lag that drifts upward on a healthy system.

| Metric | Meaning |
| --- | --- |
| `harvest.replication.lag_seconds{shard}` | The RPO in seconds. Absent when unknown. |
| `harvest.replication.lag_bytes{shard}` | WAL backlog. Survives a disconnected standby; also the disk-pressure signal for an abandoned slot. |
| `harvest.replication.standbys{shard}` | Live walsenders. `0` means replication is down. |
| `harvest.shard.generation{shard}` | The write-authority epoch. Its *skew* is the point. |
| `harvest.shard.fenced{shard}` | A worker was fenced and stopped. Never self-healing. |

Resolution is bounded below by the sampler interval: a healthy deployment
reports somewhere between zero and one interval.

Two properties worth knowing before you read the number:

- **The trail is written by the workers.** One worker per shard per tick holds a
  Postgres advisory lock and writes the watermark, so fleet size does not
  multiply the writes — but a shard with *no* running workers stops beating, and
  its reported RPO then grows with the outage rather than with the replication
  lag. A reading taken after the fleet is stopped is about downtime, not data
  loss.
- **An unreadable view emits nothing.** Without `pg_monitor` the sampler logs a
  warning and skips every DR gauge for that shard rather than publishing zeros.
  A stale series is the honest representation of "we cannot see"; a zero would
  page on-call with "replication is down" for a missing `GRANT`.

---

## Multi-shard skew

Shards fail over **independently** and will land at different points. Two
shards replicating with different lag, promoted seconds apart, produce a
cluster whose shards disagree about the last few seconds of history. Harvest
does not hide this, because the alternative — a cross-shard consistent
snapshot — is exactly the purpose-built replication machinery this design
refuses to build.

`harvest.shard.generation{shard}` makes the skew machine-readable: if
`max(harvest_shard_generation) != min(harvest_shard_generation)` across shards,
some shards were failed over and some were not.

The named hazards, all of which are the *same* hazards the restore runbook
documents:

- **An outbox `*Requested` without its cross-shard terminal.** Cross-shard
  signal, cancel, and await delivery is a two-phase affair: a `*Requested` row
  on the source shard, a terminal on the target. If the source shard failed
  over from a point *after* the request and the target from a point *before*
  the delivery, the request is replayed and re-delivered. Under the
  at-least-once contract that is legal — the outbox scanners retry by design —
  but the side effect happens twice. The reverse skew leaves a `*Requested` row
  the target already consumed; the scanner re-delivers, the target dedupes.
- **Parent/child skew.** A parent shard promoted from a later point may hold a
  `ChildWorkflowStarted` for a child whose shard was promoted from an earlier
  point and has no record of it. The parent's await never completes on its own.
  The child is re-startable by id; the parent's `child_timeout` (issue #243) is
  what stops it waiting forever.
- **Schedules.** A schedule shard promoted from an earlier point may re-fire a
  run it already fired. Start idempotency (`harvest_start_idempotency`)
  absorbs this when the schedule's runs carry an idempotency key; without one,
  the run executes twice.

The discipline is the same as the restore runbook's, and it is not optional:

> **Fence all shards, verify all shards, and only then start workers.**

Starting workers on the shards that promoted quickly while their siblings are
still promoting means live cross-shard traffic against a half-failed-over
cluster — which turns bounded, known skew into unbounded, undiagnosable skew.

---

## Related

- `docs/runbooks/cross-region-failover.md` — the procedure, the drill, and fail-back.
- `docs/runbooks/backup-restore.md` — the restore-verification checks this reuses.
- `docs/sharding.md` — how a shard maps to a database.
- `docs/runbooks/ha-deployment.md` — single-region HA, which this composes with.
