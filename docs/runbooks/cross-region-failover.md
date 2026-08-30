# Runbook: cross-region failover

**When to use this:** a shard's primary region is gone or unreachable and you
have decided to promote its standby. Failover is **operator-initiated** — there
is no automatic promotion, and this runbook is the only supported path.

**Target RTO: under 15 minutes** for a two-shard deployment, following these
steps in order.

**Before you start,** read the one-paragraph limit in
`docs/cross-region-dr.md` § *What fencing does not do*: the generation fence
makes a stale worker structurally harmless when it reaches the promoted
primary, but it cannot stop a partitioned old region from writing to its own
database. Step 1 is what stops that, and it is not optional.

Companion documents:

- `docs/cross-region-dr.md` — the topology, the design, and the honest limits.
- `docs/runbooks/backup-restore.md` — `harvest backup verify`, which step 3 reuses.

---

## The order is the safety argument

```
1. FENCE + ISOLATE   revoke the old region's write authority
2. PROMOTE           stop replicating; advance sequences
3. VERIFY            prove the promoted data is resumable
4. START WORKERS     only now
```

Every reordering is a known failure:

| Reordering | What it costs |
| --- | --- |
| Promote before fencing | The old region keeps write authority for the promotion window. Two live primaries, and any surviving worker forks a history. |
| Verify before fencing | The old region keeps writing *during* verification, so what you verified is already stale. |
| Start workers before verifying | An incoherent promotion becomes a corrupted live region, and now with new events on top. |
| Fence some shards, start workers | Live cross-shard traffic against a half-failed-over cluster. Bounded, known skew becomes unbounded skew. See `docs/cross-region-dr.md` § *Multi-shard skew*. |

**Fence every shard, verify every shard, then start workers.** Not shard by
shard.

---

### 1. Fence and isolate the old region

First, stop the old region's database from accepting writes. Do this at the
infrastructure layer — this is the step the engine cannot do for you:

```sql
-- On the old primary, if you can still reach it:
ALTER ROLE harvest CONNECTION LIMIT 0;
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'harvest';
```

If you cannot reach it, cut it off at the network (security group, firewall) or
demote it. **Do not skip this because "the region is down"** — "down" from your
vantage point and "partitioned" look identical, and the difference is whether
old-region workers are still committing.

Then bump the write-authority epoch on every standby you are about to promote:

```bash
harvest dr fence \
  --shard 0=postgres://harvest@standby-b/harvest_shard0 \
  --shard 1=postgres://harvest@standby-b/harvest_shard1 \
  --reason "region A outage 2026-08-30, INC-1234" \
  --i-understand-this-stops-the-fleet
```

This is what revokes write authority. Every worker pinned to the previous epoch
— in **either** region — is now structurally unable to claim tasks or append
events, and stops with `ShardFenced`, incrementing
`harvest.shard.fenced{shard}`.

> A fenced worker is recovered by **restarting** it against the region that now
> holds authority. **Never** re-pin or adopt the new epoch in place: a worker
> the promoted region just evicted would silently rejoin the fleet, which is
> exactly the split-brain the epoch exists to prevent.

Confirm every shard moved:

```bash
harvest dr status --shard 0=... --shard 1=... -o json
```

Every shard's `generation` must have increased, and all shards should now be at
the same value. A shard still at the old epoch is not fenced.

### 2. Promote the standby

**Logical replication** — on each standby database:

```sql
DROP SUBSCRIPTION harvest_dr_shard0;
```

**Physical replication:**

```bash
pg_ctl promote -D /var/lib/postgresql/data
```

Then, **for logical standbys, advance the sequences** — this is mandatory, not
housekeeping:

```bash
harvest dr promote \
  --shard 0=postgres://harvest@standby-b/harvest_shard0 \
  --shard 1=postgres://harvest@standby-b/harvest_shard1
```

Logical replication copies rows but **not sequence values**. Without this the
promoted primary holds every replicated `harvest_events` row while
`harvest_events_id_seq` still sits where it was when the subscription was
created, and the first append after failover dies on a duplicate key — a total,
immediate, and mystifying outage. `advance_sequences_after_promotion` sets every
sequence in the schema to match its data and prints what it set; paste that into
the incident log.

Harmless and idempotent on a physical replica, which replicates sequences
already. Run it either way rather than remembering which kind you have.

### 3. Verify the promoted region is resumable

Reuse the restore-verification checks — the replayer sample, the scanner
dry-run, and cross-shard coherence — against the promoted databases. Supply
**every** shard: a cross-shard reference into a shard you did not name is
reported as an advisory rather than checked.

```bash
harvest backup verify \
  --shard 0=postgres://harvest@standby-b/harvest_shard0 \
  --shard 1=postgres://harvest@standby-b/harvest_shard1 \
  --i-know-this-is-scratch \
  -o json
```

Exit codes: `0` clean or resumable-with-reclaim, `1` incoherent (**do not start
workers**), `2` undetermined (a shard was unreachable — resolve that first).

The verifier is read-only; each connection is pinned read-only at the session
level, so it cannot mutate the region you are about to bring up. Expect
findings that are normal after any abrupt cut-over: `RUNNING` tasks whose worker
never came back (the poison-pill reclaimer takes them), and open leases. Those
are *resumable*, not *incoherent* — `docs/runbooks/backup-restore.md` § 3
enumerates which is which.

Note the reported cross-shard skew. Shards promote from different points; see
§ *Multi-shard skew* in `docs/cross-region-dr.md` for what that means for
outbox `*Requested` rows and parent/child pairs.

### 4. Start workers

Only now. Point the fleet's DSNs at the promoted region and start it with
`dr_fencing` still enabled — each worker pins the **new** epoch at startup.

```bash
harvest worker health -o json
harvest dr status --shard 0=... --shard 1=...
```

You are done when every shard reports the new generation, workers are claiming,
and in-flight workflows are reaching terminal states.

---

## RPO: what the number means

`harvest.replication.lag_seconds{shard}` is the RPO. At failover time it says,
plainly:

> **Up to `lag` seconds of acknowledged work is lost.** Work that Harvest
> confirmed — a started workflow, a completed activity, an appended event — in
> the final `lag` seconds before the primary was lost did not reach the standby
> and does not exist in the promoted region.

And, because Harvest's execution contract is **at-least-once**:

> **Side effects from that window may re-execute.** A workflow whose activity
> completed on the old primary but whose completion event did not replicate
> will re-run that activity on the new primary. Charges, emails, and external
> calls in the lost window can happen twice. Activities that are not idempotent
> need an idempotency key; this is the same contract that already governs
> retries, not a new one — the failover just makes the window a known size.

Read the number **before** you fail over. `harvest dr status` reports it per
shard, and it is the loss you are choosing to accept.

### A missing number is worse than a large one

If the lag series is **absent** for a shard, the RPO is *unknown*, not zero. A
missing series means no standby is connected, no replication slot exists, or the
standby is further behind than the retained watermark trail. In every one of
those cases the true RPO is unbounded and growing.

Check `harvest.replication.standbys{shard}`: `0` means replication is down.
`harvest.replication.lag_bytes{shard}` still reports a real backlog when the
time lag cannot be computed, because a slot pins WAL whether or not a walsender
is attached.

If the RPO is unknown at the moment you must fail over, you are accepting an
unmeasured loss. Say so explicitly in the incident channel rather than
recording "RPO: 0".

---

## Fail-back

Failing back is a failover in the other direction, and it uses the same four
steps — with one addition at the front. Do not treat it as "just switch the
DSNs back".

**1. Re-seed the old region from the new primary.** The old region's database
is not a valid standby any more: it is a divergent fork of a history that has
since moved on. Whatever it accepted while partitioned, and whatever the new
primary did after promotion, are irreconcilable. Do not re-point replication at
it. Drop it and take a fresh base backup (or a fresh `copy_data = true`
subscription) from the *current* primary.

This is also what makes fail-back safe with respect to the epoch: the current
generation travels with the data. When region A comes back it comes back at the
**new** epoch, so any surviving region-A worker still pinned to the
pre-failover epoch is fenced there too, the moment it tries to claim or persist.
That is by design, and it is why re-seeding is not optional — a hand-repaired
old database would carry the *old* epoch and quietly re-admit those workers.

**2. Let it catch up, and watch the RPO.** Fail back when
`harvest.replication.lag_seconds` on the new primary is small and stable and
`harvest.replication.standbys` is `1`. There is no urgency: unlike the outage
that forced the failover, this one is scheduled.

**3. Drain, then run the four steps in reverse.** Stop accepting new work,
let in-flight workflows quiesce (`harvest worker drain`), then fence region B,
promote region A, verify, and start workers there. The fence bump increments the
epoch again — generations only ever go up, so a fail-back does not "restore"
generation N, it advances to N+2.

**4. Restart every worker.** Including any that never stopped. A worker pinned
to an epoch two bumps ago is fenced and stopped; a worker pinned to the current
one is fine; a worker that was somehow never fenced is the one to worry about,
and restarting it removes the question.

---

## Failover drill

Run this quarterly against a scratch topology. It is the same procedure above,
against two databases you can throw away, and it is what establishes that your
**15-minute RTO** is real rather than aspirational.

**Prerequisite:** the drill needs `wal_level = logical` on the Postgres hosting
the two "regions". The engine-behaviour half of the drill is automated and runs
in CI as `cross_region_dr_tests` in the core crate's integration suite.

### The automated half

```bash
# Two "regions" as two databases in one Postgres, wired with real logical
# replication: a real walsender, a real slot, real LSNs.
HARVEST_TEST_DATABASE_URL=postgres://postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --test integration -- cross_region_dr_tests --test-threads=1
```

That suite proves the three things the runbook depends on:

| Proof | Test |
| --- | --- |
| A fenced stale worker **cannot claim** or persist, and burns no retry | `a_fenced_worker_cannot_claim_tasks`, `a_fenced_worker_cannot_persist_events` |
| Post-promotion, in-flight workflows **resume** and reach terminal states on the new primary with no history fork | `a_promoted_standby_resumes_in_flight_work_and_rejects_the_old_region` |
| The RPO metric reports injected **lag** within ±5s of an independent measurement | `rpo_metric_reports_injected_replication_lag` |
| A disconnected standby reports bytes and an *unknown* RPO, never zero | `a_disconnected_standby_reports_bytes_and_an_unknown_rpo_never_zero` |

### The human half

What the automated suite cannot measure is *you*: whether the DSNs are where
you think, whether the runbook's commands work against your topology, and how
long the whole thing takes.

1. Stand up two "regions" (a compose file with two Postgres containers, or two
   databases in one instance as above) and replicate shard 0 from A to B.
2. Start a worker fleet against A with `dr_fencing` enabled. Start some
   long-running workflows.
3. **Start a stopwatch.** Kill region A (`docker stop`, or a firewall rule —
   prefer the firewall, because it simulates a *partition*, which is the harder
   case).
4. Run steps 1–4 above against region B.
5. **Stop the stopwatch** when in-flight workflows are completing on B. That is
   your RTO. If it is over 15 minutes, the fix is usually a missing DSN
   inventory or an unrehearsed `harvest dr fence` invocation, not the engine.
6. Now start the *old* region's workers back up, still pinned to the old epoch,
   and point them at B. They must stop with `ShardFenced` and increment
   `harvest.shard.fenced`. **Zero of them may claim a task.**
7. Practise fail-back.

Record: RTO, the RPO reported at failover, whether any old-region worker
claimed anything (must be zero), and any step where the runbook was wrong. Fix
the runbook — that is the deliverable of a drill.
