# Design — Issue #1127: GC for `harvest_rate_limit_buckets`

`harvest_rate_limit_buckets` rows are auto-registered `ON CONFLICT (key) DO
NOTHING` and are **never deleted**. Two key families embed caller/tenant input —
`dyn-rate:{expr}:{resolved}` (#699/PR #1101) and
`start-throttle:{workflow}:{key}` (#607) — so cardinality is tenant-driven and
the table grows without bound.

This adds a **shard-local, batched, opt-out sweep** to the retention janitor that
deletes buckets that are provably *inert*: idle past a configurable window, at
full effective capacity, unreferenced, and carrying no live operator override.
**No new `WorkflowEvent` variant. One migration (a supporting index only).**

---

## 1. Acceptance criteria (derived from the issue)

The issue has no numbered AC list; these are derived from its Summary/Proposal
and are what §7's test matrix is written against.

| AC | Requirement (source sentence) |
|----|-------------------------------|
| AC1 | A sweep deletes `harvest_rate_limit_buckets` rows that have been idle longer than a retention window ("Add a shard-local sweep that deletes buckets idle … for longer than a … retention window"). |
| AC2 | "**or continuously full**" — a bucket that never drains is also collected. |
| AC3 | The window is **configurable** ("a configurable retention window"). |
| AC4 | The sweep is **keyed off `last_refill_at`/`updated_at`** ("e.g. keyed off `last_refill_at`/`updated_at`"). |
| AC5 | The sweep is **shard-local** ("Keep it shard-local (buckets are per-shard)"). |
| AC6 | It is folded into an existing scanner: `timeout::enforce_timeouts_once` **or** `retention::run_shard_tick` / `RetentionRuntime::spawn` ("Natural fold-in points"). |
| AC7 | A deleted row **re-registers on next use** via `ensure_rate_limit_bucket` — deleting an idle-past-window row is safe ("An idle-past-window row is safe to delete: it re-registers on next use"). |
| AC8 | Growth is actually bounded for the unbounded key families named in the issue (`dyn-rate:`, `start-throttle:`). |

Implicit-but-binding (from the engine's own invariants, not the issue text):

| AC | Requirement |
|----|-------------|
| AC9 | The sweep must never strand work. Both the claim-time gate and `try_consume_rate_limit_token` are **fail-closed on a missing bucket row**, so deleting a bucket a live task/deferred start still depends on would wedge it. |
| AC10 | The sweep must never hand out free tokens: deletion + re-registration resets `tokens = burst`, which is a rate-limit **violation** unless the row was already at full effective capacity. |
| AC11 | A live TTL'd pacing override (#945) must not be silently destroyed. |
| AC12 | `dry_run` must delete nothing; the pass must be observable (counter + `GET /admin/retention`). |

---

## 2. Brainstorming — candidate designs

1. **TTL column + partial index**, sweep `WHERE expires_at < NOW()`. Needs a
   migration to add and backfill a column that duplicates `last_refilled_at`.
2. **Sweep in `timeout::enforce_timeouts_once`**, next to the debounce /
   start-throttle scanners. Runs at the timeout cadence (seconds), per shard.
3. **Sweep in the retention janitor** (`RetentionRuntime::spawn`), next to the
   audit / schedule-decision / summary-GC passes. Already owns "retention
   window", `batch_size`, `dry_run`, the per-shard monitor and the
   `GET /admin/retention` surface.
4. **`ON CONFLICT … DO UPDATE` LRU cap**: keep at most N buckets, evict the
   oldest on insert. Turns every enqueue into a write amplification, and evicts
   *live* buckets under load — exactly backwards.
5. **Postgres partitioning / `pg_cron`**: operator-owned cron. The repo's stated
   posture (#958) is that reclamation is *engine-automated*, not an operator
   script.
6. **Delete on last use** (refcount the task queue): needs a counter column and
   makes the hot claim path pay for GC.
7. **Prefix-scoped sweep**: only the *unbounded* families (`dyn-rate:`,
   `start-throttle:`) are eligible; bounded static activity keys are never
   collected.
8. **Fullness-gated deletion**: only delete a bucket already at full effective
   capacity, so delete+re-register is *token-neutral by construction*.
9. **Referential guards**: anti-join `harvest_task_queue` (non-terminal rows) and
   `harvest_start_throttle` before deleting.
10. **Metric + tick-result reporting** so an operator can see the sweep working
    (and see it *not* working).

**Chosen:** 3 + 7 + 8 + 9 + 10. Rejected: 1 (redundant column), 2 (see Blue hat),
4/6 (hot-path cost, wrong eviction order), 5 (operator cron).

### Why prefix-scoped (7) is not a shortcut

A **static** activity bucket (bare activity name) is registered *only at worker
startup* (`worker.rs` rate-limit bucket registration). The dynamic families
re-register **in the same transaction as the enqueue** (`worker.rs` →
`ensure_rate_limit_bucket`) and the throttle family re-registers in
`throttle::reserve_or_defer` → `ensure_throttle_bucket`. So AC7's "it
re-registers on next use" is **true for the dynamic families and false for
static keys**: collecting a static key and then enqueueing a task for it would
leave that task stalled behind the fail-closed gate until the next worker
restart. Static keys are also *bounded* (one per registered activity), so they
are not the growth the issue is about. The prefix scope is therefore both the
safe and the sufficient scope, and it reuses the family list the gauge sampler
already maintains for the identical reason (`RATE_LIMIT_GAUGE_SAMPLER_FILTER`).

---

## 3. Reverse brainstorming — how would we *break* this?

*"How could a bucket GC corrupt pacing, stall work, or lose operator intent?"*

| # | Failure we could ship | Guard |
|---|----------------------|-------|
| R1 | Delete a bucket a **PENDING** task references → the claim gate's `EXISTS` is fail-closed, the task never becomes claimable, and nothing re-registers the row. | Anti-join `harvest_task_queue` on any **non-terminal** row (`state NOT IN ('COMPLETED','FAILED','CANCELLED')` — fail-safe for future states). |
| R2 | Delete a bucket a **RUNNING** circuit-breaker task will debit at dispatch (`try_consume_rate_limit_token`, also fail-closed) or that a retry will re-queue. | Same anti-join (covers RUNNING/PAUSED). |
| R3 | Delete a bucket with **deferred throttled starts** (`harvest_start_throttle.bucket_key`) → `fire_claimed_throttle_row` can never debit, so those starts are deferred *forever* (they are not even expired unless `expires_at` is set). | Anti-join `harvest_start_throttle`. |
| R4 | Delete a **partially drained** bucket → re-registration sets `tokens = burst`, granting free capacity. A tenant could farm this by pacing requests just under the sweep window. | Delete only when **effective available tokens ≥ effective burst** (AC10), using the shared `effective_available_tokens_expr` / `effective_burst_expr` so the fullness test can never drift from the debit math. |
| R5 | Delete a bucket carrying a **live TTL'd override** (#945) → the operator's incident mitigation silently evaporates on re-registration. | `override_expires_at IS NULL OR override_expires_at <= NOW()`. |
| R6 | Collect a **static** activity bucket → stalls until worker restart (see §2). | Prefix scope. |
| R7 | Use the *baseline* `refill_rate`/`burst` for the fullness test while an override is active → wrong verdict in both directions. | Reuse the effective-value expressions (R4). |
| R8 | Unbounded `DELETE` blocking the claim path on a big table. | `LIMIT batch_size` per statement, drain-loop with a short-batch break (mirrors `purge_expired_summaries`), supporting index. |
| R9 | Per-candidate anti-joins degenerate to seq scans of `harvest_task_queue`. | New partial index on `(rate_limit_key) WHERE rate_limit_key IS NOT NULL AND state NOT IN ('COMPLETED','FAILED')`; `harvest_start_throttle` already has `idx_harvest_start_throttle_bucket`. |
| R10 | Sweep failure fails the whole retention tick, so history retention stops too. | Best-effort per shard: log + continue, exactly like the audit/schedule/summary passes. |
| R11 | `dry_run` deletes anyway. | Pass is skipped entirely under `dry_run` (it is a destructive pass, unlike partition *creation*). |
| R12 | Metric labelled by bucket `key` → one series per tenant forever, the very cardinality bug #699 already fixed for the gauges. | Label by **family** (`dyn-rate` / `start-throttle`) only. |
| R13 | Enabling GC changes behaviour for deployments that never opted in. | It is on by default *because the unbounded growth is the bug*, but every guard above means a swept row is provably inert; and `rate_limit_bucket_retention_secs: None` (or `0`) disables it outright. |
| R14 | A window so short that a bucket is collected between the enqueue and the claim. | Validated `>= MIN_RATE_LIMIT_BUCKET_RETENTION` (1 h) at build time; and R1's anti-join makes the race harmless regardless. |
| R15 | The sweep itself races a concurrent debit (`claim_task`'s `rate_limit_debit` CTE) and deletes a row that just went non-full. | The predicate lives **inside the `DELETE`'s own statement**, selecting candidates `FOR UPDATE SKIP LOCKED`; a row a concurrent writer holds is skipped, and once the sweep holds the lock no one else can modify the row before the delete lands. |
| **R16** | **The sweep's dependent anti-joins run against one snapshot, so an enqueue transaction that has not committed yet is invisible: its bucket is collected and its task, committed a moment later, is stranded forever behind the fail-closed gate.** | **The interlock in §5.0.** |

R15/R16 decide the shape of the statement: the predicate must be inside the
`DELETE` itself, its candidates must be locked, and the *ensure* path must take
that same row lock — otherwise "no live dependent" is only true as of a snapshot
that a committing enqueue immediately invalidates.

---

## 4. Six hats

**⬜ White (facts).** Table has PK `key`, `refill_rate`, `burst`, `tokens`,
`last_refilled_at`, `created_at`, `updated_at`, + three #945 override columns.
Writers: `ensure_rate_limit_bucket` (insert-only), the `rate_limit_debit` CTE in
`claim_task`, `try_consume_rate_limit_token`, `refund_rate_limit_token`, and the
override admin routes. Every debit/refund writes `last_refilled_at = NOW()`;
none of them writes `updated_at` (no trigger exists), so `updated_at` moves only
on override writes. Readers that fail closed on a missing row: `claim_task`'s
`EXISTS` gate and `try_consume_rate_limit_token`. Referencing tables:
`harvest_task_queue.rate_limit_key`, `harvest_start_throttle.bucket_key`. No
production `DELETE` exists today.

**🟥 Red (instinct).** Deleting rows that gate admission is the scariest change
in this subsystem: the failure mode is a *silently stalled tenant*, which looks
like "the workflow is slow" and takes a day to diagnose. Every instinct says
make deletion provably a no-op, not merely probably safe.

**⬛ Black (risks).** §3. The two that survive review scrutiny: free tokens on
re-registration (R4) and stalled dependents (R1–R3). Both are addressed by
predicates rather than by timing assumptions.

**🟨 Yellow (upside).** The table stops growing for exactly the families that
grow. Reuses the janitor's existing cadence, batching, dry-run, monitor and HTTP
surface, so the new operator surface is one config field, one counter and one
tick-result field. Deletion is *provably* token-neutral, so there is no pacing
story to explain in the runbook beyond "inert rows are collected".

**🟩 Green (creative).** `GREATEST(last_refilled_at, updated_at, created_at)` as
the idleness clock covers AC4 literally and also treats an override write (which
only moves `updated_at`) as activity — so an operator who sets an override on an
otherwise idle bucket keeps it. Reusing the `effective_*_expr` builders for the
fullness test means the sweep inherits every future correction to the
override-aware accrual math for free.

**🟦 Blue (process).** Fold into the **retention janitor**, not
`enforce_timeouts_once`: the timeout scanner runs on a seconds cadence on the
hot path and owns *liveness* work, whereas this is *space reclamation* on a
window measured in hours — which is the retention janitor's entire job, along
with `batch_size`, `dry_run`, the per-shard monitor and `GET /admin/retention`.
Red → Green → Refactor, with the unit-testable core (predicate SQL shape,
config validation, family classification) split from the DB-backed behavioural
tests so the RED phase is meaningful without a container.

---

## 5. Design

### 5.0 The concurrency interlock (R16)

`ensure_rate_limit_bucket` becomes:

```sql
INSERT ... VALUES (...)
ON CONFLICT (key) DO UPDATE SET updated_at = NOW()
 WHERE harvest_rate_limit_buckets.updated_at < NOW() - INTERVAL '300 seconds'
```

`ON CONFLICT DO NOTHING` takes **no lock** on the conflicting row; `DO UPDATE`
does. Paired with the sweep's `FOR UPDATE SKIP LOCKED`, the two serialise:
whichever runs second sees the other's outcome — the sweep skips a row an
in-flight enqueue holds, and an ensure blocked behind a delete simply re-inserts
the bucket. The `WHERE` keeps it off the hot path (a bucket touched within the
interval is written **zero** times), and it can never miss the case that
matters, because a bucket stale enough for the GC to consider is by definition
older than 300s < `MIN_RATE_LIMIT_BUCKET_RETENTION` (1 h) — which is why that
floor is load-bearing rather than decorative.

`throttle::ensure_throttle_bucket` delegates to the same function instead of
carrying a second copy of the statement, which would silently miss the
interlock.

Verified against two real concurrent sessions, both directions:
`an_uncommitted_enqueue_is_not_swept_out_from_under_its_own_task` passes with
the touch and **fails** with `DO NOTHING` restored (the sweep deletes the
bucket and leaves a PENDING task pointing at nothing).

### 5.1 Config (`RetentionConfig`)

```rust
/// Idle window after which an inert per-tenant rate-limit bucket is collected.
/// `None` disables the sweep. Default: 7 days.
pub rate_limit_bucket_retention_secs: Option<u64>,
```

* `with_rate_limit_bucket_retention(Duration)` / `without_rate_limit_bucket_gc()`.
* `rate_limit_bucket_retention()` → `Option<Duration>`;
  `rate_limit_bucket_gc_active()` → `bool`.
* `validate()`: window must be within `MIN_RATE_LIMIT_BUCKET_RETENTION` (1 h) …
  `MAX_MAX_AGE`, else the build fails (never silently clamped).
* `enabled()` returns `true` when the sweep is active, so an
  everything-else-off deployment still spawns the janitor.

### 5.2 The sweep (`retention::sweep_rate_limit_buckets`)

```sql
DELETE FROM harvest_rate_limit_buckets b
 WHERE b.key IN (
   SELECT key FROM harvest_rate_limit_buckets
    WHERE (key LIKE 'dyn-rate:%' OR key LIKE 'start-throttle:%')   -- AC8 / R6
      AND GREATEST(last_refilled_at, updated_at, created_at) < $1  -- AC1/AC4
      AND (override_expires_at IS NULL OR override_expires_at <= NOW())  -- R5
      AND <effective available tokens> >= <effective burst>        -- AC2/R4
      AND NOT EXISTS (SELECT 1 FROM harvest_task_queue q
                       WHERE q.rate_limit_key = key
                         AND q.state NOT IN ('COMPLETED','FAILED','CANCELLED')) -- R1/R2
      AND NOT EXISTS (SELECT 1 FROM harvest_start_throttle s
                       WHERE s.bucket_key = key)                   -- R3
    ORDER BY key
    LIMIT $2
    FOR UPDATE SKIP LOCKED)                                        -- R15/R16
 RETURNING key
```

Drains in `batch_size` batches up to `MAX_RATE_LIMIT_SWEEP_BATCHES_PER_TICK`
(50), mirroring the partition sweeper's `max_drops` budget: the first sweep of a
table that has grown for months must not turn one janitor tick into an
open-ended delete loop holding a pooled connection. Successive ticks converge.

Returned keys are classified into families in Rust for the counter. Loop until a
short batch, exactly like `purge_expired_summaries`.

### 5.3 Wiring

A new pass in `RetentionRuntime::spawn`, after the summary GC, gated on
`rate_limit_bucket_gc_active() && !dry_run`, per shard, best-effort (R10), with
`RetentionTickResult.rate_limit_buckets_deleted` reported through its own
monitor updater (the pass runs outside the history-retention phase gate, so it
cannot ride along in that phase's `update`) and
`metrics.record_rate_limit_buckets_deleted(family, count)` emitted.

### 5.4 Migration

`<utc>_harvest_rate_limit_bucket_gc`: one partial index on
`harvest_task_queue (rate_limit_key) WHERE rate_limit_key IS NOT NULL AND state
NOT IN ('COMPLETED','FAILED','CANCELLED')` (R9). No column changes; `down.sql` drops it.

---

## 6. What this deliberately does **not** do

* Does not collect bounded static activity buckets (§2).
* Does not touch `harvest_events` — no invariant exception is added (`CLAUDE.md`
  §"Engine Invariants" is unchanged).
* Does not coordinate across shards (AC5).
* Does not add an admin endpoint; the existing `GET /admin/retention` carries
  the new counter (its contract entry is `free_form`).

---

## 7. TDD plan

**RED (unit, no DB)** — `retention.rs` `#[cfg(test)]`:
config default/validate/accessors/`enabled()`; the rendered SQL contains each
guard clause and reuses the shared effective-value expressions; family
classification.

**RED (behavioural, Postgres)** — `tests/integration/rate_limit_bucket_gc_tests.rs`:
AC1 idle+full collected; AC2 continuously-full collected; AC1-negative recently
used retained; R4 partially-drained retained; R1/R2 PENDING and RUNNING
referenced retained; R3 deferred throttled start retained; R5 live override
retained, expired override collected; R6 static key retained; AC7 re-registration
after collection admits work again (end-to-end, no stall); AC3 window honoured;
AC12 dry-run collects nothing; batching converges over ticks; metric + tick
result reported.

**GREEN** then **REFACTOR**: the family list became one shared constant with a
test asserting the gauge sampler's exclusion filter agrees with it, and
`throttle::ensure_throttle_bucket` was collapsed onto
`queue::ensure_rate_limit_bucket` so the interlock cannot be missed by one of
the two registration paths.

Result: 13 unit tests + 15 DB integration tests, all green, plus the full
`retention`, `throttle`, `rate_limit_key`, `admission_gate_authoritative`,
`legal_hold` and docs/hygiene suites re-run unchanged.
