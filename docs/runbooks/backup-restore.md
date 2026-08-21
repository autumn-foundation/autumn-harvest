# Backup, restore, and post-restore resumability verification

Harvest keeps every durable fact — event histories, task queue rows, timers,
signals, leases, outbox rows — in Postgres. A Harvest backup is therefore a
Postgres backup. What *is* Harvest-specific is the question this runbook
answers:

> The restore succeeded. Is the fleet **safe to resume**, and what will happen
> the moment I start workers?

The tool that answers it mechanically is `harvest backup verify`. Everything in
this runbook is designed so a full restore drill — restore, verify, decide —
fits inside 30 minutes.

- [1. Choosing a backup approach](#1-choosing-a-backup-approach)
- [2. Post-restore semantics: what resumes, what re-runs](#2-post-restore-semantics-what-resumes-what-re-runs)
- [3. The 30-minute restore drill](#3-the-30-minute-restore-drill)
- [4. `harvest backup verify`](#4-harvest-backup-verify)
- [5. Reading the report](#5-reading-the-report)
- [6. Multi-shard restore skew](#6-multi-shard-restore-skew)
- [7. What this tool does not do](#7-what-this-tool-does-not-do)

---

## 1. Choosing a backup approach

Harvest's tables fall into three classes, and they want different things from a
backup. **You still take one backup of the whole database** — the classes tell
you how much *recency* matters, and therefore whether a nightly logical dump is
enough or you need point-in-time recovery.

| Class | Tables | What a stale copy costs you | Recommended |
|---|---|---|---|
| **Durable execution truth** — append-only, irreplaceable | `harvest_workflow_executions`, `harvest_events` | Lost work. An event that existed at T but not in the backup is a workflow whose recorded past is *wrong*; replay reconstructs a different run. | **Physical + PITR** (WAL archiving / `pg_basebackup`, or your provider's PITR). RPO should be seconds, not hours. |
| **Derived dispatch state** — regenerable-ish, but only from truth | `harvest_task_queue`, `harvest_timers`, `harvest_signals`, `harvest_sessions`, `harvest_mutex_locks`/`_waiters`, `harvest_completion_deliveries`, `harvest_start_throttle`, `harvest_debounce` | Stalled or duplicated dispatch, healed by the scanners (§2). Cheap relative to lost history. | Same backup as above. Do **not** try to restore these selectively. |
| **Operator configuration** — low churn, high blast radius | `harvest_schedules`, `harvest_build_policies`, `harvest_build_compat`, `harvest_api_tokens`, calendars | A schedule silently reverting to an old cadence; a build policy re-routing new starts to a retired build. | Also keep a **`pg_dump` of just these tables** on a slower cadence. It is your "what was the config on Tuesday?" artifact, and it diffs readably. |

**`pg_dump` vs physical/PITR, plainly:**

- **`pg_dump` (logical)** is a *consistent snapshot at one instant*. It is
  excellent for cloning a scratch database, for config diffing, and for
  cross-version moves. It is a poor primary backup for the durable-truth class
  because your RPO is "however long ago the dump ran".
- **Physical + WAL archiving (PITR)** lets you restore to an arbitrary
  timestamp. This is the primary recommendation for any Harvest deployment
  running work you cannot afford to re-derive.

Whichever you use, the restore target for a drill is always a **scratch**
database, never production. `harvest backup verify` enforces that (§4).

### Restore mechanics that matter to Harvest

`pg_restore --data-only --disable-triggers` (and COPY-based table-at-a-time
restores) **suppress foreign keys during load**. That is what makes a *torn*
restore — a `harvest_task_queue` row whose execution never landed — physically
possible even though the live schema forbids it. `harvest backup verify` probes
for exactly that class (`dangling_task_execution`, `dangling_event_execution`);
in a schema-and-data restore with constraints enforced, those probes are
expected to return zero. Run them anyway: they are the cheapest possible check
and the failure they catch is silent.

---

## 2. Post-restore semantics: what resumes, what re-runs

This is the section to read before you start workers.

### 2.1 Activities are at-least-once. Side effects after T will re-run.

Say it plainly: **any activity that completed after your restore point `T` will
execute again.** Harvest's durability contract has always been at-least-once
for activity execution — a crash between "the activity's side effect landed"
and "its `ActivityCompleted` event committed" re-runs the activity. A restore to
`T` is that same window, widened to `now - T`.

Concretely, for every activity whose `ActivityCompleted` event was written after
`T`:

- The restored history does not contain that event.
- On resume, replay reaches that activity's schedule point and dispatches it.
- The activity body runs **again**, against the *current* outside world.

Anything that is not idempotent — a charge, an email, an outbound webhook, a
non-idempotent PUT — will be repeated. Before resuming a restore with a
non-trivial `now - T`, decide per activity whether that is acceptable, and use
the idempotency keys you already have (`ctx.info().execution_id` +
`activity_id`, which are stable across attempts) to make it a no-op downstream.

Workflow code itself is *not* re-executed as a side effect — it is replayed
deterministically. The re-execution risk lives entirely in activities (and in
completion callbacks, §2.4).

### 2.2 In-flight artifacts: what each one does on resume

Everything below is **expected** in a healthy restore. `harvest backup verify`
classifies each as `reclaimable` — it reports them so you know the shape of the
resume, not because anything is wrong.

| Artifact in the restore | Verify class | What happens when workers start |
|---|---|---|
| `harvest_task_queue` row `RUNNING` with a `worker_id` that no longer heartbeats | `dead_worker_running_task` | The **poison-pill orphan sweep** (issue #367) reclaims it. Under the strike threshold it is re-queued and re-dispatched; at/over the threshold it is quarantined to the DLQ. Reclaim keys on *worker liveness*, so an un-timed task is recovered too. |
| Task past its `start_to_close` / `heartbeat` / `schedule_to_start` / `schedule_to_close` deadline | `timed_out_task` | The **timeout scanner** enforces it exactly as it would have live: `ActivityTimedOut` is appended and the task fails per its retry policy. |
| Execution past its `deadline_at` (issue #243) | `workflow_deadline_expired` | The execution-timeout scanner seals it `TIMED_OUT` on the first tick. This is not a restore artifact — the deadline genuinely elapsed. |
| Schedule holding a `fire_claim_token` whose `fire_claimed_until` has passed (issue #350) | `expired_schedule_claim` | The claim is expired, so a healthy replica re-claims the slot on the next tick. No firing is lost; at most one slot fires late. |
| `harvest_sessions` row `ACTIVE` whose host worker is gone (issue #606) | `expired_session_lease` | The session reclaim sweep marks it `BROKEN` and fails its member activities **non-retryably** (`error_type: "SessionBroken"`). A hard-pinned task on a dead host can never fail over, so the workflow must re-establish a fresh session. |
| `harvest_mutex_locks` row past its lease (issue #691) | `expired_mutex_lease` | The lease is reclaimed and the lock is granted to the next FIFO waiter. **Caveat:** the engine fences the lock *table* (`lock_seq`), not your external side effects. |
| `harvest_completion_deliveries` row `INFLIGHT` (issue #605) | `inflight_completion_delivery` | The lease lapses and the delivery is **re-attempted**. The POST may already have been received. **Receivers MUST dedupe on `delivery_id`**, which is stable across redeliveries by design. |
| `External*Requested` event with no terminal (issues #244/#492/#757) | `pending_external_request` | The external outbox scanner retries delivery. Same-shard requests resolve inline; cross-shard ones go through the outbox. If the target is genuinely absent, it fails as `target_unknown` after the grace window. |

### 2.3 What does **not** heal itself

| Verify class | Meaning |
|---|---|
| `dangling_task_execution` / `dangling_event_execution` | A row references an execution that is not in the restore. Torn restore. No scanner repairs this. |
| `external_target_missing` | A `*Requested` event names a target execution absent from the shard that owns it. |
| `child_execution_missing` | A parent awaits a child whose execution row is absent from its shard. The parent will park forever. |
| `child_terminal_rolled_back` | The parent recorded the child's terminal, but the child's shard was restored to an *earlier* point where the child is still running. See §6 — this is the signature multi-shard skew failure. |
| `wedged_schedule_claim` | A schedule with a claim token but a **NULL** `fire_claimed_until` — a torn claim pair. The scheduler claims on `fire_claim_token IS NULL OR fire_claimed_until < NOW()`, which such a row matches neither half of, so the schedule never fires again. Un-claim it by hand (`UPDATE harvest_schedules SET fire_claim_token = NULL, fire_claimed_until = NULL WHERE id = …`) before starting workers. Contrast `expired_schedule_claim`, which self-heals. |
| `replay_divergence` | A sampled history no longer replays against the deployed workflow code. Not caused by the restore; caused by a code/history mismatch. Fix by rolling *back* the workflow code, then resume (see `nondeterminism-block.md`). |

A report containing any of these exits **1**. Do not start workers.

### 2.4 Completion callbacks and outbound effects

Completion callbacks (issue #605) are the one *outbound* side effect that is not
an activity. A delivery that was `INFLIGHT` or `PENDING` at `T` will be
re-attempted after the restore, and a delivery that had already succeeded after
`T` will be attempted **again** (the restore does not know it succeeded). This
is the direct analogue of §2.1. `delivery_id` is stable across redeliveries
precisely so receivers can make this a no-op — verify your receivers actually
dedupe on it before you rely on it.

---

## 3. The 30-minute restore drill

Run this on a cadence. The point is not that the restore works; it is that the
**resume** works.

```bash
# 1. Restore into scratch. Never into production. (~5-15 min, mostly waiting.)
createdb harvest_scratch
pg_restore -d harvest_scratch /backups/harvest-2026-08-20.dump
#   ...or your provider's PITR clone, targeting a fresh instance.

# 2. Verify resumability. Read-only. (~seconds to a minute.)
#    Pass --live-dsn (once per live shard) so the guard can actually refuse a
#    DSN that resolves to production. Do NOT reflexively pass
#    --i-know-this-is-scratch: it DISABLES that check, and a run that is not
#    guarded says so on stderr.
harvest backup verify \
  --shard 'postgres://…@scratch-host/harvest_scratch' \
  --live-dsn 'postgres://…@live-host/harvest'

# 3. Decide, from the exit code:
#      0 -> resumable. Start workers.
#      1 -> INCOHERENT. Do not start workers. Restore again from a different point.
#      2 -> UNDETERMINED. The drill did not actually check. Fix and re-run.
#
#    Note: the shipped CLI reports `replay: NOT VERIFIED` -- it links no
#    application workflow handlers. See §4.3 for the embedder recipe that
#    adds replay coverage.
```

Multi-shard (§6) adds one rule: **restore all shards, verify all shards, then
start workers.** Never start a worker on a partially-restored fleet.

Record the drill's `generated_at`, exit code, and the finding counts. A drill
you did not write down is a drill you cannot compare against next quarter.

---

## 4. `harvest backup verify`

```
harvest backup verify --shard <[SHARD_ID=]DSN> [--shard …] [flags]
```

| Flag | Default | Meaning |
|---|---|---|
| `--shard <[N=]DSN>` | *(required, repeatable)* | A scratch DSN. Optionally prefixed `N=` to declare its shard id (e.g. `--shard '1=postgres://…'`). Unprefixed takes its **positional** index (first `--shard` is `0`, second is `1`, …), so prefix explicitly whenever your shard ids are not `0..n`. |
| `--live-dsn <DSN>` | `$HARVEST_DATABASE_URL` | A live DSN to guard against. **Repeatable — supply one per live shard.** A live shard you do not name here is not guarded against. |
| `--i-know-this-is-scratch` | off | **Disables** the live-DSN guard entirely. Use it only when you have already confirmed by other means that every `--shard` target is a throwaway. |
| `--format text\|json` | `text` | `json` is the machine-readable report (AC2d). |
| `--replay-sample <N>` | `50` | How many non-terminal histories to replay per shard. `0` disables replay. |
| `--worker-stale-secs <N>` | `60` | Heartbeat age past which a worker counts as dead. |

The live DSN for the guard comes from `HARVEST_DATABASE_URL` (or `--live-dsn`).
When no live DSN is supplied at all, or when `--i-know-this-is-scratch` is
passed, the guard does not run and the command prints a `WARNING:` line on
stderr saying so. Silence means the guard genuinely ran.

### 4.1 It is read-only, mechanically

This is enforced three ways, not by convention:

1. **Every connection issues `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY`
   immediately on connect.** Any write attempt fails with SQLSTATE `25006`. This
   is a Postgres-level guarantee, not a code review promise.

   > **Caveat — connection poolers.** The pin is *session*-scoped. Under
   > PgBouncer in `transaction` or `statement` pooling mode the session is not
   > yours between transactions, so the pin may not apply to later statements.
   > Point `--shard` at the scratch database **directly**, not through a
   > transaction-pooling proxy. (`session` pooling mode is fine.) The pin also
   > does not restrict temporary tables — verify creates none, but that is a
   > code property, not a server-enforced one.
2. **No mutating scanner is ever called.** The AC asks for the scanners to "run
   once and report what they would reclaim". Those scanners *mutate*. So verify
   reuses their **selection predicates verbatim** — the same
   `orphaned_running_tasks_query()`, `heartbeat_timeout_query()`,
   `broken_session_candidates_query()`, `expired_leases_stmt()` constants the
   live scanners use — and reports the rows they *would* claim, without claiming
   them. Reusing the same `const` string is what makes this drift-proof: if a
   scanner's predicate changes, this report changes with it.
3. **The live-DSN guard** refuses to run against a database matching your live
   config unless you explicitly acknowledge. The comparison **fails closed** —
   an unparseable DSN is treated as *matching*, so a malformed connection string
   can never sneak past the guard.

The one write it performs is none. If you point it at production without the
ack, it refuses; with the ack, it still only reads.

### 4.2 What it checks

- **(a) Replay.** Samples up to `--replay-sample` non-terminal executions per
  shard and replays each through `WorkflowReplayer` in **canary** mode. Canary,
  not strict, because a parked in-flight run legitimately suspends at its
  recorded frontier — strict replay would flag every healthy run in the fleet as
  divergent. Reported as clean / divergent / skipped.
- **(b) Scanner reclaim preview.** The predicate reuse above, reported per class
  with exact counts and bounded samples.
- **(c) Referential coherence.** Every task row's execution exists; every event
  row's execution exists; every non-terminal external `*Requested` has a live
  target (or a documented outbox path); every awaited child exists on its shard
  and has not been rolled back behind its parent's recorded terminal.
- **(d) A machine-readable report** (`--format json`) with a nonzero exit on any
  failed check.

### 4.3 Replay honesty — read this before trusting a `clean` verdict

**The shipped `harvest` CLI cannot replay your workflows.** It is a generic
binary; it does not link your application's `#[workflow]` handlers, so its
replayer is always empty. Every sampled history is therefore recorded as
`replay_skipped_no_handler`, and the report prints:

```
  replay: NOT VERIFIED — 12 sampled, 12 skipped (no handler), 0 unreadable.
```

That is the truth, not a failure: skipping is **not** a divergence, and the
report never claims coverage it does not have. But it does mean checks (b), (c)
and the cross-shard checks are what the CLI actually gives you. `clean` from the
CLI means *"the data is coherent and every stuck artifact has a scanner that
heals it"* — it does **not** mean *"your deployed workflow code still replays
this history"*.

To get check (a), call the library from a binary that links your handlers:

```rust
use autumn_harvest::backup_verify::{ShardTarget, VerifyOptions, verify_restore};
use autumn_harvest::testing::WorkflowReplayer;

let replayer = WorkflowReplayer::new()
    .register_fn("onboarding", onboarding_handler)
    .register_fn("fulfil_order", fulfil_order_handler);

let targets = vec![ShardTarget::new(0, std::env::var("SCRATCH_DSN")?)];
let report = verify_restore(&targets, &VerifyOptions::default(), &replayer).await;
std::process::exit(report.status.exit_code());
```

A workflow type with no registered handler is still skipped, never called a
divergence — so a partially-registered replayer degrades honestly too. Check the
`replay:` line before treating any `clean` verdict as covering workflow code.

---

## 5. Reading the report

Four severities, and only two of them fail the drill:

| Severity | Exit | Meaning |
|---|---|---|
| `reclaimable` | 0 | Expected. A running scanner heals it. Every healthy restore has these. |
| `advisory` | 0 | Worth knowing; not a blocker (skew, uninspected shard, skipped replay). |
| `incoherent` | **1** | The restore is torn. **Do not start workers.** |
| `undetermined` | **2** | The check could not run. You do **not** have a clean bill of health — you have no bill of health. |

Four verdicts map onto those:

| Verdict | Exit | Do this |
|---|---|---|
| `clean` | 0 | Start workers. |
| `resumable_with_reclaim` | 0 | Start workers. Expect the listed reclaim on the first scanner tick. |
| `incoherent` | 1 | Do not start workers. Restore from a different point and re-verify. |
| `unavailable` | 2 | Fix the cause (unreachable shard, or `probe_failed` — usually a restore that produced an **unmigrated or empty** database) and re-run. |

**Why `undetermined` outranks everything, including `incoherent`:** a probe that
could not run found nothing *because it did not look*. The canonical case is a
"restore" that produced a database with no Harvest schema — every probe errors on
a missing table, every condition reads as absent, and a naive tool reports a
beautiful clean bill of health on an empty database. That is the single most
dangerous output this tool could produce, so "we could not tell" is never
allowed to render as a pass.

---

## 6. Multi-shard restore skew

In a sharded deployment each shard is an independent Postgres database with an
independent backup. Nothing makes their restore points line up. **Skew is the
default failure mode, not an edge case.**

### 6.1 The invariants at risk

Harvest's per-workflow ACID guarantees are *shard-local* by design — a
workflow's events, tasks, timers, and signals all live on one shard. The
invariants that span shards are exactly the cross-workflow ones, and those are
what skew breaks:

1. **Parent/child terminal ordering.** A parent on shard A records
   `ChildWorkflowCompleted` for a child on shard B. Restore A to `T1` and B to
   `T0 < T1`: the parent believes the child finished, and the child is still
   running (or does not exist). The parent resumes past a result the child will
   now produce a *second* time. Verify class: `child_terminal_rolled_back` /
   `child_execution_missing`.
2. **External signal / cancel / await delivery** (issues #244/#492/#757). Caller
   on A recorded `ExternalSignalDelivered`; the target on B was rolled back past
   receiving it. The signal is silently lost — no scanner retries a request the
   caller already considers delivered. Verify class: `external_target_missing`.
3. **Completion-trigger fan-out and the cross-shard outbox** (issue #605). An
   outbox row on A relays a start onto B. Skew either re-relays an already-started
   target (idempotent, fine) or loses a relay whose source terminal was rolled
   back (not fine).

### 6.2 The fencing procedure

The rule is one sentence: **restore every shard, verify every shard, and only
then start any worker.**

```bash
# 1. Restore ALL shards into scratch. Target the SAME point in time.
#    With PITR, use the same recovery target timestamp for every shard.
for s in 0 1 2; do
  pg_restore -d "harvest_scratch_$s" "/backups/shard-$s-2026-08-20.dump"
done

# 2. Verify ALL shards in ONE invocation. This is the load-bearing step:
#    cross-shard coherence can only be checked when every shard is supplied.
harvest backup verify \
  --shard '0=postgres://…/harvest_scratch_0' \
  --shard '1=postgres://…/harvest_scratch_1' \
  --shard '2=postgres://…/harvest_scratch_2' \
  --live-dsn 'postgres://…@live-host/harvest'

# 3. Only on exit 0: start workers — all shards at once, not one at a time.
```

One class of external request is unadjudicable by design: a **business-key
addressed** signal/cancel (`ctx.signal_external_workflow_by_id`, issue #751)
resolves its target at *delivery* time, so the recorded event carries no
execution id to look up. Verify reports these as `workflow_id_target_unchecked`
(advisory) with the target keys, rather than silently passing over them — check
by hand that the named `(workflow_name, workflow_id)` pairs have a live run.

Do **not** verify shards one at a time in separate invocations. A single-shard
run cannot see the other side of a cross-shard reference; it reports
`uninspected_shard_reference` (advisory) rather than the `child_terminal_rolled_back`
(incoherent) it would have found. Supplying every shard is what turns an advisory
into a verdict.

Starting workers on a partially-restored fleet is worse than downtime: workers on
the restored shards will dispatch work whose cross-shard counterparts do not yet
exist, converting a recoverable skew into new, wrong, durable history.

### 6.3 Skew reporting

Verify computes the newest event timestamp on each shard and reports
`restore_point_skew_secs` — the spread across the fleet. Beyond the threshold it
raises `restore_point_skew` (advisory). Treat a nonzero skew as a prompt to look
hard at the cross-shard classes; treat a skew larger than your longest child
workflow as a reason to restore again.

---

## 7. What this tool does not do

Explicitly out of scope (issue #943):

- **Taking or scheduling backups.** This verifies a restore; it does not create
  one. Use your provider's tooling or `pg_dump`/`pg_basebackup` on your own
  cadence.
- **Continuous cross-region replication or failover fencing.**
- **Repairing anything.** Every finding is reported, never fixed. Repair is an
  operator decision — usually "restore again from a different point", not a
  surgical edit to durable history.
- **Selective restore of individual workflows.** Harvest's coherence guarantees
  are per-database; there is no supported way to restore one workflow.

## See also

- [`nondeterminism-block.md`](nondeterminism-block.md) — what to do about a
  `replay_divergence` finding.
- [`ha-deployment.md`](ha-deployment.md) — the schedule-claim mechanics behind
  `expired_schedule_claim`.
- [`triage-pending-tasks-idle-workers.md`](triage-pending-tasks-idle-workers.md) —
  if work still is not moving after a clean resume.
- [`synthetic-incident-drills.md`](synthetic-incident-drills.md) — where the
  restore drill fits among the other regular drills.
- [`../sharding.md`](../sharding.md) — the shard-local guarantee boundary §6
  reasons about.
