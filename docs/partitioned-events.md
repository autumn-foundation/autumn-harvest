# Partitioned `harvest_events`

**Issue #958.** An opt-in physical layout for `harvest_events` that lets
retention reclaim space by **dropping partitions** instead of deleting rows.

Nothing here changes what expires, when it expires, what an event looks like, or
how replay works. Only the physical layout and the reclamation mechanism change,
and only for deployments that opt in.

---

## The problem it solves

`harvest_events` is one append-only heap with a `BIGSERIAL` primary key, and
retention reclaims space through the `ON DELETE CASCADE` from
`harvest_workflow_executions`: collecting an expired execution deletes its event
rows one at a time.

At sustained volume that is the classic Postgres failure mode. A retention pass
deletes millions of rows, leaving dead tuples that bloat the heap and *every*
index, and driving autovacuum pressure that competes with the task-claim query
and the append hot path. The choices are all bad: run retention aggressively and
live with constant vacuum churn degrading p99; run it rarely and query a huge
table whose indexes have degraded; or hand-roll partitioning outside the engine
and break the schema the migrations own.

Dropping a partition is a metadata operation: O(1), no dead tuples, no vacuum
debt.

## Measured

At 20,000 executions × 10 events (200,000 rows) across 20 daily cohorts, 50%
expired — reproduce with
`cargo bench -p autumn-harvest --features db --bench retention_reclaim_bench`:

| layout | event reclamation | event-row `DELETE`s | dead-tuple ratio after | append p99 (quiet → during pass) | load ops/s |
|---|---:|---:|---:|---|---:|
| unpartitioned | (inside the pass) | 98,950 | 15.82% | 2.19 → 2.13 ms (−2.4%) | 190 |
| partitioned | 0.154 s | **0** | **0.00%** | 3.56 → 2.93 ms (−17.7%) | 176 |

Both layouts reclaim exactly the same 100,000 events. The mechanism changes; the
outcome does not. Neither layout's concurrent p99 degrades during the pass —
comfortably inside the ±5% budget.

**Read the p99 column down, not across.** Each arm runs against its own fresh
container, so the *quiet* numbers are not a controlled comparison between
layouts; the sound comparison is each arm's own quiet → during-pass change,
which is what the budget is about. The partitioned layout does cost something on
append — tuple routing, the cohort `DEFAULT`, and the trigger's cross-partition
uniqueness probe (one two-key index probe per partition per row), against the
removed foreign-key check — and the ~7% throughput difference above is the
closest this benchmark gets to measuring it. Sizing the partition count down
(below) is what keeps it small.

`HARVEST_BENCH_SCALE=full` runs the issue's headline 10M-row scale against a
real server.

### What the numbers do *not* say

The **whole retention pass** takes about the same wall time on both layouts
(≈62 s for 10,000 collected executions in the run above). Partitioning removes
the event-storage cost — from "inside the pass" to 99 ms, and from ~100,000 row
deletes to zero — but the pass is dominated by a different, pre-existing cost:
the **per-execution candidate loop**, which visits each expired execution
individually to fire the `HistoryArchiver` hook (#345), re-check legal holds
(#747) under a row lock, demote to a summary (#752), and clean up auxiliary
rows. That loop is required by the archiver contract (the hook must see each
execution before its rows become unreachable), is unchanged by this work, and
costs the same on both layouts.

So issue #958's Success Metric splits in two:

- **Met**: zero row-level deletes, dead-tuple ratio far under 5%, concurrent
  p99 regression under 5%, and event reclamation that no longer scales with row
  count.
- **Not met by this change alone**: "a pass reclaiming ≥ 50% of executions
  completes in < 60 s" at 1M executions. At the measured ~6.2 ms per candidate
  that loop needs roughly 50 minutes for 500,000 executions **on either
  layout**. Batching it is tracked separately; it is orthogonal to partitioning
  and would benefit unpartitioned deployments identically.

---

## How it works

### The partition key is `cohort`, not `timestamp`

The obvious key is wrong. Postgres requires the partition key to appear in every
`UNIQUE` constraint, so partitioning on `timestamp` would turn

```sql
UNIQUE (workflow_exec_id, event_id)
```

into

```sql
UNIQUE (workflow_exec_id, event_id, timestamp)
```

— silently destroying the per-execution id uniqueness that **is** the engine's
optimistic-concurrency detector. Two workers advancing the same workflow would
stop colliding. `timestamp` is also back-datable by operator tooling, which makes
it unsafe as a routing key.

The key is a dedicated `cohort` column: the row's **append instant**, floored to
a fixed width (one UTC day by default) by a plain column `DEFAULT`. Two
properties follow, and the whole design rests on them.

**1. Uniqueness is preserved — by the constraint *and* a trigger.** Postgres
requires the partition key in every unique constraint, so the table constraint
becomes `UNIQUE (workflow_exec_id, event_id, cohort)` — which, because `cohort`
is an append instant, is only unique *within* a cohort. That is not enough: the
engine's optimistic-concurrency detector *is* that constraint, and immediately
after converting a populated shard the gap would be systematic (every
pre-cutover row carries the `-infinity` sentinel, so any re-append for a
pre-existing execution lands in today's cohort). The insert trigger therefore
also rejects a duplicate `(workflow_exec_id, event_id)` **across** partitions —
one two-key index probe per partition per inserted row — and raises the original
constraint name, so callers cannot tell the layouts apart from the error.

> **Known limit.** Two appends of the same `event_id` that are *simultaneously
> in flight* — neither committed when the other's check runs — and whose inserts
> land in different cohorts would still both succeed: neither transaction can
> see the other's uncommitted row, and a partitioned unique index cannot span
> partitions. Closing it would mean serialising the append hot path per
> execution, risking lock-order deadlocks against the advisory locks the
> admission and mutex paths already take. The window is the overlap of two
> conflicting in-flight appends *and* a cohort boundary falling between their
> insert instants — microseconds per cohort, against a split-brain that is
> itself rare.

**2. Past partitions are sealed.** A cohort's range is a window of wall clock
that has already closed, and the `DEFAULT` can only produce a cohort at or after
"now". No future `INSERT` can route into a partition whose upper bound is in the
past. Once the sweeper proves a closed partition holds no live execution's rows,
nothing can race an append into it before the drop — the safety argument is
structural, not a lock.

### Why a `DEFAULT` and not a trigger

The first iteration stamped `cohort` from the owning execution's `created_at` in
a `BEFORE INSERT` trigger, so every event of one execution would land in one
partition. **Postgres forbids that.** Tuple routing happens *before* row triggers
fire, so a trigger that changes the partition key fails with `moving row to
another partition during a BEFORE FOR EACH ROW trigger is not supported` — and,
worse, silently *succeeds* whenever the pre- and post-trigger destinations happen
to coincide (both the `DEFAULT` partition, say), which looks like it works.

So an execution's history genuinely can span partitions. That is the trade issue
#958 anticipates, and the drop gate is exact about it.

### The drop gate

A closed partition is droppable when **no row in it belongs to a still-existing
execution**. Two tiers answer that:

1. **Fast path** — `NOT EXISTS (SELECT 1 FROM harvest_workflow_executions WHERE
   created_at < <partition upper bound>)`. An execution cannot have appended a
   row before it existed, so if nothing predates the partition's upper bound,
   nothing that could own a row in it survives. One index probe on
   `idx_harvest_we_created_at`. This is the steady state, because retention
   collects oldest-first.
2. **Exact path** — only when the fast probe says "maybe": a semi-join proving
   no row in the range has a live owner, bounded by a `statement_timeout` so one
   huge partition cannot stall a tick. A timeout retains and retries — an
   unfinished proof is not a proof.

### What a drop locks

The gate runs unlocked, so its answer can be stale by the time the drop starts.
The drop therefore re-proves it — and the lock it re-proves it under is
`SHARE` on the **one child partition**, not `ACCESS EXCLUSIVE` on the parent:

- `SHARE` conflicts with `ROW EXCLUSIVE`, so no INSERT, UPDATE or DELETE can
  touch that partition while the proof runs. That is the whole guarantee the
  re-check needs, and appends to every other cohort — which is all of them,
  this one being closed — are unaffected.
- `SHARE` does not conflict with `ACCESS SHARE`, so readers are unaffected.
  That matters more than it looks: the insert trigger's cross-partition
  `(workflow_exec_id, event_id)` uniqueness check reads the partitioned parent
  and so locks *every* child in `ACCESS SHARE`. An exclusive lock on any one
  child would stall every append on the shard, whichever cohort it belongs to.

`ACCESS EXCLUSIVE` is taken by the `DROP` alone — on the child, and on the
parent to update its partition descriptor. That is a catalog change: immediate.
The distinction is not academic. Waiting for a lock is bounded by
`drop_lock_timeout_secs`; the re-check that follows it is bounded by
`exact_scan_timeout`, and a `lock_timeout` bounds *acquiring* a lock, never
holding one. Proving a large partition under an exclusive lock would stall the
shard for the length of the scan, once per drop attempt, on every tick.

A drop that deadlocks with a concurrent append into its own closed cohort, or
that cannot get its lock in time, is reported blocked and retried next tick.

**Legal holds (#747), per-type retention overrides (#737) and long-running
executions need no special-casing at all.** Each keeps its execution row alive,
which keeps its rows owned, which blocks the drop. There is no second copy of
the retention policy here to drift out of sync with the janitor's.

### The `HistoryArchiver` hook is untouched

Reclamation never runs ahead of archival. The ordinary candidate loop archives,
summarizes and deletes each expired execution exactly as before; only then does
the sweeper find a cohort whose rows are all orphaned and drop it. A failed
archive leaves the execution in place — and therefore leaves its partition in
place too.

---

## What it costs

### No foreign key

The partitioned layout drops `harvest_events_workflow_exec_id_fkey`, because
that FK's `ON DELETE CASCADE` **is** the delete storm being eliminated.

Its insert-time half is restored by a validate-only trigger
(`harvest_events_require_execution`): an event may still not be written for an
execution that does not exist. Same cost and same lock as the FK — a
primary-key probe taking `FOR KEY SHARE`. The lock is not optional: without it
the probe is an observation rather than a guarantee, and an append racing a
retention delete could observe the execution, let the delete commit, and then
commit an orphan itself.

What is deliberately *not* restored is the delete-time cascade. Deleting an
execution leaves orphan event rows, and the sweeper is their garbage collector.
Orphans are invisible to every read path in the engine: all of them filter by a
`workflow_exec_id` the caller already resolved to a live execution.

### Reads do not prune

History reads filter on `workflow_exec_id`, not on `cohort`, so each one probes
every partition's index. **Keep the live partition count small.** The rule:

```
partitions ≈ retention horizon ÷ cohort width  +  lookahead (default 3)  +  2
```

The `+ 2` is the `DEFAULT` partition and — after any non-empty conversion — the
legacy partition, which survives until every pre-cutover execution is gone. With
a one-day cohort and a 7-day horizon that is ~13.

**Aim for ~8–16, not more.** Every non-pruning statement takes `AccessShareLock`
on the parent, each partition, *and* each index it scans — roughly `2N+2`
relation locks. Postgres reserves 16 fast-path lock slots per backend
(`FP_LOCK_SLOTS_PER_BACKEND`); past that, every acquisition goes through the
shared lock-manager hash, and the cost scales with connection count. PostgreSQL
18 makes those slots scalable; on 14–17 it is a real ceiling. If your retention
horizon is 90 days, use a weekly cohort (`--cohort-width-secs 604800`), not a
daily one.

Two per-request paths are more partition-sensitive than `load_history`, because
they touch `harvest_events` once per *row* rather than once per call:
`usage.rs`'s `LEFT JOIN LATERAL` for last-event timestamps (behind
`/admin/usage`), and `quota.rs`'s `history_bytes` sum (on the **workflow-start
admission path**). Both multiply their existing per-iteration index probe by the
partition count — another reason to keep it small.

### A long-running execution pins the cohorts it wrote into

Its siblings' rows in those cohorts are reclaimed late. The opt-in
`straggler_grace_secs` setting adds a targeted `DELETE` of *orphan rows only* in
a cohort pinned for longer than that. It is **off by default**, so the default
configuration never issues a row-level delete against `harvest_events`.

**This changes the blast radius of a legal hold, and that is a compliance
question, not only a disk-space one.** On the unpartitioned layout a hold
retains one execution's rows. Here, a held execution keeps every cohort it wrote
into alive — with the default one-day cohort, a 30-day run means ~30 cohorts —
and every *other* execution's rows in those cohorts are retained with it, across
workflow types and tenants, however long ago they expired. Retention still
reports them as deleted, because their execution rows are gone; it is the event
rows that linger.

If a deployment makes a bounded "history is deleted after N days" commitment,
set `straggler_grace_secs` so that commitment stays bounded by something the
operator controls rather than by the longest-lived hold. `harvest partition
status` reports which cohorts are pinned and why.

### Postgres version

Requires **Postgres 14 or later** for the partitioned layout, which is stricter
than the engine's own floor. Declarative partitioning with a default partition
and metadata-only `ADD COLUMN … DEFAULT <constant>` arrive in 11, but
`ALTER TABLE … DISABLE TRIGGER` on a partitioned parent only recurses to its
partitions from 14 — and the `DEFAULT`-partition drain depends on that. (The
drain also disables the cloned trigger on each partition explicitly, so the
sequence is correct on older versions too; 14+ is the version this is tested
against.)

---

## Enabling it

The migration (`20260728000000_harvest_event_partitioning`) is **inert**. It
ships the cohort function, the `cohort` column, the drop gate's index and the
integrity trigger, but does not convert anything. Existing deployments keep the
ordinary table and byte-for-byte identical behaviour until an operator opts in.

### Greenfield or small table

```bash
harvest partition enable --shard "$DSN" --i-understand-the-lock-window
```

One transaction under a bounded `lock_timeout`, so a failure leaves the
deployment exactly as it was. Instant on an empty table.

On a populated table the existing table is attached **whole** as the pre-cutover
partition — no row is copied or rewritten — but the two index builds and the
constraint validation happen inside the lock window. Fine for tables in the
low millions; not fine for a busy ten-million-row one.

### Large live table

```bash
harvest partition plan > convert.sql   # review it, then run it step by step
```

The plan moves the expensive work out of the lock window:

| Step | What | Lock |
|---|---|---|
| 1 | Bake the cohort width into `harvest_event_cohort` | none |
| 2 | Drop any index a failed build left invalid, then `CREATE UNIQUE INDEX CONCURRENTLY` ×2 | none that blocks appends |
| 3 | `ADD CONSTRAINT ... NOT VALID`, then `VALIDATE CONSTRAINT` | brief `ACCESS EXCLUSIVE` for the catalog row, then `SHARE UPDATE EXCLUSIVE` for the scan — readers and writers proceed |
| 4 | Rename, create parent, attach legacy | **`ACCESS EXCLUSIVE`, metadata only — seconds** |
| 5 | Hand back to the engine | none |

Step 3 is what lets step 4's `ATTACH PARTITION` skip its own full-table
verification scan. Steps 3 and 4 are each bounded by an explicit
`lock_timeout`, so a conversion that cannot get the lock **fails** rather than
stalling the deployment behind it. Step 3 needs that bound as much as step 4
does: `NOT VALID` skips the table scan but still takes `ACCESS EXCLUSIVE` to
write the catalog row, and one idle-in-transaction reader is enough to make the
request queue — with every append arriving after it queued behind the `ALTER`.
Re-run the step after clearing the blocker.

**The partitioned layout is not compatible with a flat logical-replication
standby.** `enable`, the CLI and step 1 of the plan all refuse to convert while
any publication covers `harvest_events` — which the DR runbook's `CREATE
PUBLICATION harvest_dr FOR ALL TABLES` produces. There are two independent
breaks, and only the first looks like a configuration problem:

1. **Leaf names.** `publish_via_partition_root` defaults to false, so the rows
   publish under their *leaf* partition names. The standby — provisioned by the
   inert migrations, since logical replication carries no DDL — has only the
   flat `harvest_events`, and the apply worker stops on the first event.
2. **Deletes stop being replicated at all.** The partitioned layout drops the
   `ON DELETE CASCADE` foreign key on purpose, so deleting an execution no
   longer deletes its events; the rows go away when their partition is dropped,
   and `DROP TABLE` is DDL, which is never replicated in any configuration. The
   subscriber's own cascade cannot cover for it either — apply runs with replica
   trigger behaviour, under which referential-integrity triggers do not fire. So
   the standby's `harvest_events` keeps every event forever and
   `harvest backup verify` reports `DanglingEventExecution`, an **Incoherent**
   finding: do not start workers. The standby stops being failover-capable.

`ALTER PUBLICATION harvest_dr SET (publish_via_partition_root = true);` fixes
only the first, which is exactly the trap — the subscription keeps applying
while the copy quietly stops being restorable. The workable configuration is the
partitioned layout on **both** sides with `publish_via_partition_root = true`,
where the standby's own maintenance reclaims once it is promoted. Having done
that (or established that the publication is not feeding a Harvest standby),
override the refusal with `EnableOptions::allow_incompatible_publications`
(CLI: `--allow-incompatible-publications`).

**Step 3 is re-runnable, including after a failed validation.** Its two
statements are separate transactions so the validation scan can fail fast
rather than queue every append behind it — which means the constraint can be
present and unvalidated. Re-running the step finishes the validation instead of
failing on the constraint that is already there.

**A lost `CONCURRENTLY` build is expected, and step 2 is re-runnable.** A
cancelled or failed `CREATE INDEX CONCURRENTLY` leaves the index behind marked
invalid, and `IF NOT EXISTS` would report success on a re-run without noticing.
Step 2 drops invalid copies of its own two indexes before rebuilding them, and
step 4 refuses to open its lock window unless both are present and valid —
because `ATTACH PARTITION` cannot reuse an invalid index and would build a
replacement with the exclusive lock already held, turning a metadata-only
window into a full index build.

**Grants and ownership are carried across.** Every conversion path replaces
`harvest_events` with a freshly created table, and `CREATE TABLE ... LIKE`
copies no ACLs and no owner. Both `enable`, the scripted plan and `disable`
replay the original table's owner and table-level `GRANT`s onto the
replacement, so a deployment that runs migrations as one role and connects the
engine as another keeps the `SELECT`/`INSERT` the preflight check requires.
Column-level grants are not replayed; nothing in the engine issues them.

**Recheck the cutover before running step 3.** The plan bakes in the value
computed when it was printed. A *stale* (older) cutover is safe — every
pre-conversion row carries the `-infinity` sentinel and still falls inside the
legacy range — but it leaves the cohorts between then and now with no partition,
so their rows land in the `DEFAULT` partition until maintenance drains them. A
cutover in the *future* is not safe.

Until step 4 commits, nothing exists but two extra indexes and one `CHECK`
constraint, all droppable with no downtime.

### Rolling back

```bash
harvest partition disable --shard "$DSN" --i-understand-this-rewrites-the-table
```

Copies every surviving row back into a plain table and restores the foreign key.
This **rewrites the whole table** — schedule a window.

---

## Operating it

### Maintenance is automatic

The retention janitor runs a full maintenance pass every tick (hourly by
default): drain the `DEFAULT` partition, extend the lookahead window, sweep
droppable cohorts. **No operator cron pre-creates partitions.** A shard that has
not opted in pays one cheap catalog query per tick and nothing else.

Ordering is deliberate — drain first (rows parked in `DEFAULT` block creation of
the partitions that would cover them), then create (so a tick that drops a
backlog still leaves the write window covered), then sweep (so a cohort freed
earlier in the same tick is reclaimed now rather than next time).

Partition maintenance counts as retention work in its own right, so the runtime
spawns for it even when every retention *horizon* is off (`max_age`, audit and
schedule purging all disabled). Partition creation is not reclamation: an
operator who deliberately retains everything forever still needs the write
window extended, or every append ends up in the `DEFAULT` partition. Set
`partitions.enabled = false` to opt out — but then nothing maintains the layout,
and `harvest partition maintain` has to run on a schedule instead.

### The `DEFAULT` partition

Always present, normally empty. An append whose cohort has no partition lands
there instead of failing with `no partition of relation found`, which would
stall a live workflow on a maintenance gap. Maintenance drains it back into real
cohort partitions.

It is never dropped, and it sorts last in the sweep so a bounded pass can never
spend its budget reaching it.

### "Why has space not come back?"

```bash
harvest partition status --shard "$DSN"
```

The sweeper reports every cohort it considered and left alone, with the reason:

- `a live execution still owns rows` — something in that cohort is retained: a
  run still in flight, a legal hold, a longer per-type override, or a row not
  yet past its horizon. Find it with a `created_at`-ranged query on
  `harvest_workflow_executions`.
- `ownership scan exceeded its budget` — the exact gate timed out; it retries
  next tick. Persistent, on a very large partition, means raising
  `exact_scan_timeout` or narrowing the cohort width.
- `lock not acquired, or an owner appeared before the drop` — either a writer
  or a competing maintenance pass is holding the partition, or the
  authoritative re-check taken under `SHARE` found an owner that appeared after
  the gate ran. Expected under load, and self-correcting.
- `unbounded upper bound` — a partition with no upper bound, which this engine
  never creates. It means something else attached one by hand; it can never be
  swept.

The same information is on every retention tick's per-shard status
(`partition_maintenance.sweep.blocked`).

### Tuning

`RetentionConfig::partitions`:

| Field | Default | Notes |
|---|---|---|
| `enabled` | `true` | For incident response, not tuning: disabling it stops both partition creation and reclamation. |
| `lookahead_cohorts` | `7` | Cohorts kept pre-created ahead of now. |
| `max_drops_per_tick` | `32` | Each drop ends in a brief `ACCESS EXCLUSIVE` catalog lock; a bounded budget keeps a backlog from holding the append path off. Successive ticks converge. |
| `drop_lock_timeout_secs` | `2` | Fail fast and retry rather than making every append queue behind the sweep. |
| `exact_scan_timeout_secs` | `15` | Budget for the exact ownership scan, reached only when more than `owner_probe_cap` old executions survive. |
| `owner_probe_cap` | `1000` | How many surviving old executions the cheap narrow probe enumerates before falling back to the exact scan. |
| `straggler_batch` | `1000` | Rows per straggler `DELETE` statement. |
| `straggler_grace_secs` | `None` | Opt-in orphan `DELETE` for cohorts pinned by long-running executions. Off means zero row-level deletes. |

`dry_run` suppresses only the *sweep*. Partition creation and the `DEFAULT`
drain still run — they delete nothing, and a dry-run deployment that stopped
extending its write window would end up appending into `DEFAULT` indefinitely.

---

## Scope

Per-shard by construction: a shard is a database, and each shard's layout is
detected at runtime, so a **half-converted cluster is a supported state**. There
is no cross-shard coordination and none is needed.

Not covered: partitioning `harvest_task_queue` or any other table; changing
retention policy semantics; automatic in-place conversion.
