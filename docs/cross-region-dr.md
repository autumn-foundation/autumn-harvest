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

### When the fence turns on

The fence is **on by default wherever DR is configured** (issue #1823). The
default mode is `Auto`. You do not need to set anything.

```rust
use autumn_harvest::replication::DrFencing;

// The default. Shown only for clarity.
let config = WorkerConfig::default().with_dr_fencing_mode(DrFencing::Auto);
```

At startup each process probes every shard database it serves for a **DR
marker**. A marker is any one of these:

- a `harvest_shard_generation` row, which a fenced process or
  `harvest dr fence` writes;
- a replication slot whose name starts with the DR prefix (`harvest_dr`);
- a subscription in this database whose name or slot name starts with the
  DR prefix.

The slot test uses the same scope as the RPO metric. A logical slot counts
for its own database only. A physical slot counts for every database on the
cluster, because physical replication copies the whole cluster.

| Mode | No marker found | Marker found |
| --- | --- | --- |
| `Auto` (default) | Runs unfenced. | Provisions an absent row, pins every shard, runs fenced. |
| `Enabled` (`with_dr_fencing(true)`) | Provisions the row, pins, runs fenced. | Provisions an absent row, pins, runs fenced. |
| `Disabled` (`with_dr_fencing(false)`) | Runs unfenced. | **Refuses to start.** |

A process whose configuration disagrees with the database does not start.
The log line says which rule it broke, and what to change:

- `Disabled` on a DR database.
- A fenced process that cannot name the shard it serves.
- A fenced process whose shard number differs from the row in the database.
  A shard database holds its own row only. One exception: a process on a
  single database may share it with processes for other logical shards. It
  logs a warning and provisions its own row beside theirs.
- A fenced process on a DR **standby**: a database with a logical
  subscription of any name, or a server in recovery. No Harvest process
  writes to a standby. The runbook starts processes only after promotion.

**Colocated shards.** Several logical shards can share one database, and
one claim scan there serves them all. The scan is not filtered by shard, so
a claim checks the pin of every logical shard on that database. A fence on
any one of them stops claims for all of them. This holds across processes
too: a process pins the rows of other logical shards that it finds on its
database at startup. Provision every row before the first process starts.
A process that finds a row it did not pin stops its claims and background
writes on that database, and logs why. It cannot tell whether that shard
was fenced. Restart it to pin the row.

**Shard identity.** A pin needs a shard number. A worker takes it from its
sharded pool or from `with_shard_assignments`. With neither, the process
reads it from the database: exactly one `harvest_shard_generation` row names
the shard. Zero or several rows give no answer, and the process refuses to
start.

**What `Auto` cannot see.** Some DR setups leave no marker until the first
fence. A physical standby that follows the primary with no `harvest_dr` slot
(the `application_name` fallback above) is one. So are replicas that an HA
manager such as Patroni or a managed cloud service creates. For these, set
`with_dr_fencing(true)` on every process, or run one process with it once.
That process provisions the row, and the row then marks the database for
`Auto`.

**Cost.** A database with no marker pays one probe per shard at startup: two
catalog reads. The claim query stays the byte-for-byte pre-#954 statement.
The persist path issues no extra statement, and no DR sampler starts.

**A shard the probe cannot reach.** The process never guesses that such a
shard is plain:

- A worker **holds** the shard. It starts, registers and serves its other
  shards, as before (issue #961). The held shard is pinned to a sentinel
  generation that no row holds, so the worker claims nothing there and every
  append there fails closed. No worker task writes there either: no fleet
  row, heartbeat, rate-limit bucket or monitor tick. The database may be an
  unpromoted standby. A background task probes again with backoff. A
  shard with no marker is released and runs unfenced. A shard with a marker
  stops the worker, because a pin is fixed for the life of a process. The
  restarted worker pins it.
- A shard this process already pinned keeps that pin. The runner pins
  before its worker starts, so a brief outage does not hold or stop it.
  The worker must use the pool that took the pin. A worker on another pool
  refuses to start: that pool can reach another database, such as a
  logical standby.
- A process that pins DR shards refuses a worker whose databases carry no
  DR marker. The pins are process-wide, so that worker would check its
  writes against another database. The reverse order is refused too: a
  process that runs an unfenced worker refuses a fenced one. Run each in
  its own process, or set `DrFencing::Enabled` on both. A worker that fails
  to start does not count: its mode is free again.
- A worker's startup writes (its fleet row and its rate-limit buckets) and
  each heartbeat run under the fence barrier. A held or fenced shard gets
  none of them, and they retry later.
- A worker that finds its pin superseded stops, and so does every other
  worker in its process: the pins are process-wide. Its heartbeat and every
  activity heartbeat flusher stop at once. Its shutdown skips its database
  writes: fleet status,
  sticky-pin release, claim release and the lease keeper. Another region
  owns those rows. Its orphan reclaim recovers the claims. Each shutdown
  write also holds the shard's fence barrier, because the sampler stops
  with the worker. A bump during shutdown therefore stops these writes too.
- A fenced worker with an assigned shard it cannot reach refuses to start.
  It cannot pin that shard.
- A fenced worker holds an unassigned shard it cannot reach, and it serves
  its assigned shards. A cross-shard write to the held shard fails closed.
  When that shard returns, the worker stops, and the restart pins it.
- `HarvestRunner::start` retries the probe with backoff for a few seconds,
  then refuses to start.

With the fence on, each worker **pins** every assigned shard's
`harvest_shard_generation` epoch at startup. It does this before it registers
in the fleet and before its first poll. If the worker cannot read the epoch,
it **refuses to start**. It never runs unfenced.

The sampler interval and slot prefix are still worker settings:

```rust
let config = WorkerConfig::default()
    .with_replication_sample_interval(Duration::from_secs(15));
```

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
  the region that *does* hold authority picks them up. Every claim variant
  applies the same gate: by kind, by id, and the batched claim
  (`claim_task_batched`, issue #1340). A unit test reads `queue.rs` and fails
  when a new `claim_task*` entry point skips it.
- **Persist assert.** Every append takes the generation row `FOR SHARE` first.
  `FOR SHARE` is the load-bearing detail, not defensive noise: the fence bump
  takes the same row exclusively, so it cannot commit while an in-flight
  persist holds it, and any persist that begins after it commits sees the new
  epoch and fails. That is a commit-order barrier, not a racy read.

Promoting a standby is therefore: bump the epoch on the new primary, and every
worker still pinned to the old one is *structurally* unable to claim or append.
It stops with `HarvestError::ShardFenced` and increments
`harvest.shard.fenced{shard}`.

### Admin tooling

Admin writes go through the same check as worker writes (issue #1823).

- **Management API.** `HarvestRunner::start` pins every storage shard before
  it serves a request. That covers API-only nodes, which run no worker. Every
  mutating management API, Vantage, MCP and webhook route then runs
  `assert_fence` for each pinned shard, before its handler. A fenced node
  answers `503` and the handler does not run. Read routes still answer, so
  you can inspect the node. Most `harvest` CLI commands call this API, so
  they are fenced too. An embedder that mounts `harvest_api_router` without
  `HarvestRunner::start` must call `replication::pin_process_fence` at
  startup. Otherwise the node pins nothing and checks nothing.
- **Direct-database CLI writes.** `harvest partition enable|maintain|disable`
  and `harvest shard rebalance|rebalance-resume|reconcile-migrated-seals`
  connect to shard databases directly. On a DR database they need
  `--expect-generation <N>`. Read `N` from `harvest dr status` against the
  region that holds authority. The command refuses a shard at any other
  generation. A stale DSN to a demoted primary then writes nothing. A pin
  taken at connect time cannot catch that case. A rebalance dry run reads
  only, so it needs no flag. A standby carries the primary's generation, so
  the epoch alone cannot reject it. Every command therefore refuses a server
  in recovery. A rebalance writes rows, so it also refuses any database with
  a subscription, whatever its name.
  The partition commands may run on a logical standby: logical replication
  carries no DDL, so the partitioned layout must be built on both sides.
- **In-process partition maintenance.** Each pass on a shard holds a fence
  barrier for its whole run, across its several transactions.
- **Retention and the batch executor.** Each retention tick and each batch
  executor tick holds a fence barrier on every pinned shard. A fenced or
  held shard skips the whole tick, and the log names the reason.
- **The scheduler.** Each scheduler pass on a shard holds the same barrier
  while it writes `harvest_schedules` and fires. A fenced or held shard gets
  no write, and the log names it.

The barrier is a transaction on its own connection. It takes the shard's
advisory lock in shared mode and checks the generation. `harvest dr fence`
takes that shard's lock in exclusive mode before it bumps, so the bump waits
for a pass in flight on that shard. A pass that starts after the bump sees
the new generation and stops. Each shard has its own lock. A pass on one
shard does not delay a bump of another shard on the same database.
The bump waits at most 5 seconds. A pass that runs longer makes the bump
fail with a lock timeout. Run `harvest dr fence` again. After the bump gets
the lock, it waits 2 more seconds before it commits. A pass that lost its
barrier stops in that time, and a short write it sent commits first.

These direct-database commands are exempt, by design:

| Command | Why it is not fenced |
| --- | --- |
| `harvest dr fence` | It moves write authority. Fencing it would block the failover. |
| `harvest dr promote` | It runs during the failover, before workers start. It only advances sequences. |
| `harvest migrate run` | Logical replication carries no DDL. You must migrate both regions. |
| `harvest backup verify`, `harvest dr status`, `harvest partition status`, `harvest migrate status` | They read only. |

An admin write holds the same fence barrier as a scheduler pass. The
management API holds one per pinned shard until the handler returns. A
rebalance or a partition command holds one for every shard with a row on
each database it changes, at the stated epoch, until that database's work
ends. It also holds `harvest_shard_generation` in `SHARE` mode, so no new
shard row can appear mid-command. A colocated shard with no stated epoch is
refused. The runner's startup trigger sync holds one too. A cross-shard
completion-trigger relay runs after its caller returns, so it holds its own
barrier on its source and target shards. A bump therefore cannot commit
while any of them writes. A read route takes no barrier, but some write
audit rows, such as the event stream. Each audit write checks the fence in
its own transaction, so a stale node writes no audit row.

Three limits, stated plainly:

- A barrier opens one extra connection per database. Concurrent operations
  hold at most 64 of these at once. An operation takes the slots for all of
  its databases at once, so it never holds some while it waits for more. An
  operation waits up to 10 seconds for its slots, then fails closed: an
  admin write answers `503`. An operation over more than 64 databases must
  still guard each of them at once. It takes every slot and runs alone, so
  the process then holds one guard connection per database, and it logs a
  warning. Size `max_connections` for that on a process with more than 64
  shard databases. On a DR node
  every admin write, scheduler pass and partition pass pays that cost. The
  barrier pings that connection each second. If the session ends, or a
  ping takes more than 3 seconds, the pass counts the barrier as lost. The
  server ends a barrier session that sends no ping for 10 seconds, so a
  node cut off from the database frees its lock. If the session ends, the
  server frees the lock. The pass then stops: a scheduler pass before it
  fires, a partition pass or a rebalance at once, with an error. An admin
  write or a webhook answers `503`. Before it stops, the pass ends the
  backend of each connection it holds, so the server rolls back a
  statement it already runs. That covers pooled connections and the direct
  connection of a `harvest partition` command. It does so on a connection
  of its own, outside the pool. That happens inside the bump's 2-second
  wait. A checkout inside a pass waits at most 2 seconds, on any shard's
  pool. If it gets no connection, the pass stops, so a busy pool cannot
  hold a bump off. If the process
  cannot end a backend in time, it logs a warning, and that statement can
  still commit after a bump. History appends are not
  exposed: each checks the fence in its own transaction.
- The check reads every shard of the storage pool, and every pinned shard
  colocated with one, on each admin write. If one cannot be read, every
  admin write on the node answers `503`. That fails closed. A node that has
  lost authority on one shard has lost it on the failover the runbook
  performs for all shards.
- `--expect-generation N` covers every `--shard` in a command. After the
  runbook, all shards share one generation. Shards at different generations
  take `--expect-generation <ID>=<N>`, once per shard. A per-shard value
  overrides `N`. The CLI probes with the default `harvest_dr` prefix. With a
  custom prefix, always pass the flag.

### Invariants

- **No new `WorkflowEvent` variant.** Fencing writes no workflow history. A
  fenced attempt appends nothing — not a marker, not a rejection event. Replay
  is untouched, and a history recorded before a failover replays identically
  after it.
- **No change to any existing table.** Two additive tables, both empty until a
  fenced process provisions them.
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

Three smaller limits, stated plainly:

- **The fence covers Harvest processes only.** Workers and the management
  API fence themselves by default on a DR database, and a process configured
  `Disabled` there does not start. Your own scripts that write to Harvest
  tables, and the exempt commands in § *Admin tooling*, are not fenced. Your
  *tooling* must respect the failover too.
- **A process decides at startup.** A process started before the database had
  a DR marker runs unfenced until it restarts. After you first configure
  replication or run `harvest dr fence --provision`, restart the fleet.
- **A bump fences everyone.** Workers in the new region pinned to the old epoch
  are fenced exactly as old-region workers are. That is why the runbook's order
  is fence → promote → verify → **then** start workers, and why bumping a
  generation on a healthy shard is a fleet-wide outage recovered by restarting
  the fleet, never by bumping again.

---

## Why failover is not automatic

Automated failover is **out of scope** (issue #1823). Failover stays
operator-initiated, for four reasons:

1. **Isolation is the safety step, and the engine cannot do it.** Step 1 of
   the runbook cuts the old primary off at the network or the role. A
   promoter inside Harvest cannot reach a partitioned region to do that.
2. **"Down" and "partitioned" look the same from one side.** A promoter that
   acts on a failed health check promotes during a partition too. Then two
   primaries accept writes, and histories fork.
3. **A safe promoter needs a quorum witness in a third failure domain.**
   Harvest has no such component, and adding one breaks the "no new
   infrastructure in core" rule above.
4. **Shards fail over at different points.** An unattended promoter would
   start workers shard by shard. The runbook forbids that order; see
   § *Multi-shard skew*.

If you need unattended failover, use a Postgres HA manager that has a
witness (for example Patroni with etcd, or your cloud's managed failover).
Make it run the runbook in order. Isolate the old primary first. Then fence
and promote: fence first on a logical standby, and promote first on a
physical standby, which is read-only until promotion. Then run
`harvest dr promote` and `harvest backup verify`, and start workers last.

Two rules matter more under automation:

- **Do not publish the new endpoint until `harvest dr fence` completes on
  every shard.** A worker that follows a DNS or VIP flip before the fence
  still holds the old epoch, and that epoch is still current.
- **Set `with_dr_fencing(true)` on every process.** `Auto` cannot see the
  replicas that HA managers and managed services create. See § *When the
  fence turns on*.

After the fence, a worker that reconnects to the promoted primary with the
old epoch cannot claim or append.

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

### Scoped to one WAL stream per generation

The setup SQL above replicates `harvest_replication_heartbeat` along with
every other table (`FOR ALL TABLES`), so a standby can carry beats the OLD
primary wrote before a promotion. Those LSNs belong to a WAL stream a
promoted primary does not share, so they are not comparable to its own
positions. Each beat is stamped with the `harvest_shard_generation` epoch in
force when it was written, and the RPO reads only beats from the CURRENT
epoch — a beat from a superseded generation cannot be mistaken for one in the
current WAL stream. See the fail-back section of
`docs/runbooks/cross-region-failover.md` for what an operator sees during a
promotion.

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
