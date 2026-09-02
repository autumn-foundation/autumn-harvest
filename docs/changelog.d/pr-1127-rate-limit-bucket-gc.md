## Phase — Idle rate-limit bucket GC (issue #1127)

`harvest_rate_limit_buckets` rows are auto-registered `ON CONFLICT (key) DO
NOTHING` and, until now, were **never deleted** — no production `DELETE`
existed anywhere in the tree. Two of the key conventions that share that table
embed caller/tenant input: `dyn-rate:{expr}:{resolved}` (per-key activity rate
limits, #699/PR #1101) and `start-throttle:{workflow}:{key}` (workflow-start
throttles, #607). Cardinality there is caller-driven and unbounded, so the table
grew one row per tenant key forever.

**The sweep.** `queue::sweep_idle_rate_limit_buckets` — shard-local, batched,
folded into the retention janitor next to the audit / schedule-decision /
summary-GC passes, gated on the new `RetentionConfig`
`rate_limit_bucket_retention_secs` (default **7 days**, floor 1 hour,
`without_rate_limit_bucket_gc()` to disable). It sits deliberately *outside* the
history-retention phase gate — bucket growth is driven by dispatch traffic, not
by how long finished histories are kept.

**A collected bucket is provably inert, not merely probably safe.** Every guard
lives inside the single `DELETE`, because a bucket can be debited concurrently
and predicates resolved by an earlier `SELECT` would already be stale:

- **Family scope.** Only the two unbounded families, from the new
  `queue::UNBOUNDED_RATE_LIMIT_KEY_PREFIXES` — the same list that keeps those
  families out of the per-key gauge sampler (#699), since "unbounded per tenant"
  is one judgement seen from two sides. A **bounded static** activity bucket is
  deliberately not collectable: it is re-registered only at worker startup, so
  collecting one would stall the next enqueue behind the fail-closed claim gate
  until a restart — whereas both dynamic families re-register in the same
  transaction as the work that needs them, which is what makes the issue's "it
  re-registers on next use" true.
- **Idle** past the window on `GREATEST(last_refilled_at, updated_at,
  created_at, last_registered_at)`, so a debit or refund, an operator or config
  write, a (throttled) re-registration, and a bucket created but never used all
  count as activity.
- **Full.** Effective available tokens at or above the effective burst, derived
  from the *same* `effective_available_tokens_expr` / `effective_burst_expr` the
  debit path uses. Deleting a partially drained bucket would hand out free
  capacity, because re-registration resets `tokens = burst`; with this, delete +
  re-register is **token-neutral by construction** rather than by timing luck.
- **No live TTL'd override** (#945), and **no permanent operator baseline**: a
  bucket written through `POST /admin/rate-limits/{key}` (#332) carries
  `baseline_set_at` and is exempt. That route validates nothing against the
  registry, so an operator clamping a noisy tenant targets exactly these
  per-tenant keys — and collecting one would silently revert the clamp to the
  code-declared rate the next time that tenant appeared. Deliberate operator
  intent is never destroyed by a background pass.
- **No live dependent.** Anti-joins against non-terminal `harvest_task_queue`
  rows and against `harvest_start_throttle`. Both the claim-time gate and
  `try_consume_rate_limit_token` fail **closed** on a missing bucket, and
  nothing re-registers a bucket for an already-enqueued task or an
  already-deferred start, so collecting one of those strands the work forever.
  The task predicate is `NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')` so a
  future task state fails safe toward *retaining*.

**Both collectable namespaces are now reserved** against a static bucket key
(`builder::RESERVED_RATE_LIMIT_KEY_PREFIXES`, the macro's compile-time reject,
and both worker-side guards), checked on the **effective** key — `rate_limit_key`
when set, otherwise the activity *name*, which is what a static bucket is
registered under, so a hand-built `ActivityInfo` named like a generated bucket
cannot squat one either. #699 reserved `dyn-rate:` and
left `start-throttle:` as a follow-up, which was fine while a squat was a
namespace nit; #1127 makes it a stranding bug, because the GC collects that
namespace on the guarantee that everything in it re-registers with the work that
needs it — true for a generated key, false for a static one.

**The concurrency interlock (the P1 this design's own reverse-brainstorm
surfaced).** The anti-joins cannot see an enqueue whose transaction had not
committed when the sweep took its snapshot — that bucket would be collected out
from under a task about to reference it, and stranded permanently.
`ensure_rate_limit_bucket` therefore runs a **conditional** `UPDATE ... SET
last_registered_at = NOW() WHERE key = $1 AND <row is stale>` ahead of its
`INSERT ... ON CONFLICT DO NOTHING`, and the sweep selects `FOR UPDATE SKIP
LOCKED`. A plain `UPDATE` locks only the rows its qualification matches, so a
bucket registered within `RATE_LIMIT_BUCKET_TOUCH_INTERVAL_SECS` (5 min) — the
hot path — is neither written nor locked, while a bucket stale enough for the GC
to consider (always, given the window's 1-hour floor) is locked and refreshed.
Whichever of the two runs second sees the other's outcome: the sweep skips a
locked row, and an ensure that lost the race re-inserts the bucket behind the
delete.

`ON CONFLICT DO UPDATE` would have been the obvious way to take that lock and is
the wrong one: Postgres locks the conflicting row *before* evaluating the `DO
UPDATE`'s `WHERE`, so even a no-op touch holds an exclusive row lock for the
whole decision transaction — measured at a 5-second stall on a concurrent
`claim_task` debit of the same bucket, and enough to deadlock two enqueues
touching the same two buckets in opposite order. The registration list is also
handed over sorted by key, so the rare stale-path locks are always taken in one
deterministic order (the precaution the quota advisory locks already take,
#946). `throttle::ensure_throttle_bucket` delegates to that one function instead
of carrying a second copy of the statement, which would have silently missed the
interlock — and `reserve_or_defer` now ensures the bucket **before** its FIFO
backlog guard rather than inside the no-backlog branch. The append path
previously touched the bucket not at all, so an observed backlog row that the
scanner drops between the check and the insert (a `schedule_to_start` stale-out
debits no token) left the sweep seeing an idle, full, dependent-free bucket
while the pending row committing a moment later referenced a bucket that no
longer existed — deferred forever behind a fail-closed debit.

The touch writes a dedicated `last_registered_at` rather than reusing
`updated_at`: `updated_at` means "an operator or config write changed this
bucket" and `GET /admin/rate-limits` reports it as such, so letting ordinary
dispatch traffic move it would have destroyed that answer.

**Bounded work per tick.** Up to `MAX_RATE_LIMIT_SWEEP_BATCHES_PER_TICK` (50)
`batch_size` batches per shard, mirroring the partition sweeper's drop budget,
so the first sweep of a table that grew for months cannot turn one janitor tick
into an open-ended delete loop; successive ticks converge.

**Observability.** `RetentionTickResult.rate_limit_bucket_gc` (per shard, via
`GET /admin/retention`, whose contract entry is already `free_form`) plus
the counter `harvest.retention.rate_limit_buckets_deleted`, labeled by the
bounded `family` and **never** by the bucket key — labelling by the key would
trade an unbounded table for an unbounded metric series, the same call made for
the per-key gauges (#699) and `harvest.codec.reencrypted` (#948). New dashboard
panel and rows in `docs/telemetry.md` and ADR-0001 §7.

**Observability, and what a zero means.** Each shard reports a
`RateLimitBucketGcOutcome` — collected totals per family, whether the tick was a
`dry_run` forecast, and the error when the pass could not run. `None` means the
GC is switched off, which is deliberately distinct from "it ran and everything
was live" and from "it could not run": a bare counter collapses all three into
one indistinguishable zero, and a permanently-failing shard would then look
exactly like an idle one. And `dry_run` runs the pass as a **read-only preview**
built from the sweep's own predicates, paged by a keyset cursor so its forecast
covers the same per-tick budget a real pass spends (deleting nothing, it would
otherwise re-read its first batch and under-report by up to 50×), rather than
skipping it — for a collector
that is on by default, "preview it first" is the standard derisking move, and it
was precisely what a skip made unavailable.

`GET /admin/rate-limits` gains both new columns on its row shape
(`baseline_set_at`, `last_registered_at`), so "why has this bucket never been
collected?" and "why not yet?" are answerable from the same read an operator is
already looking at.

**Upgrade safety.** The migration backfills both new columns rather than leaving
them null, because a null in either is indistinguishable from a fact the GC
needs. `baseline_set_at` is stamped on every row whose `updated_at` moved after
creation — before this release only an operator or config write could do that —
so a per-tenant clamp set through `POST /admin/rate-limits/{key}` *before* the
upgrade is exempt rather than silently reverted on the first sweep.
`last_registered_at` is stamped on every existing row with the migration's own
clock, which makes each ineligible for a full retention window: the GC's
interlock is taken by the *writer*, a worker on the previous binary does not take
it, and the janitor starts collecting the moment the first upgraded instance
runs — so without this a rolling restart would have a new janitor sweeping while
old workers still write unprotected. The grace is one-time, so abandoned
per-tenant buckets are still reclaimed a window later.

**One migration** (`20260902133132_harvest_rate_limit_bucket_gc`): two additive
nullable columns (`last_registered_at`, `baseline_set_at`) and one partial index,
`idx_harvest_task_queue_rate_limit_key_live`, backing the non-terminal-task
anti-join. The pre-existing `idx_harvest_task_queue_rate_limit_key` is partial on
`state = 'PENDING'` and cannot serve it — a RUNNING task (a circuit-breaker
activity debits at dispatch, not at claim) and a retry-bound one pin their
buckets just as hard; the migration carries the `CREATE INDEX CONCURRENTLY`
recipe every other index migration on a hot table in this tree carries. No data
migration, **no `WorkflowEvent` variant**, and nothing writes to
`harvest_events` — the invariant list in `CLAUDE.md` gains no fourth exception.
`GET /admin/preflight` now probes for `DELETE` on `harvest_rate_limit_buckets`:
without that, a least-privilege role passes startup and then fails every sweep
with a warning and a zero count, indistinguishable from "nothing was eligible"
while the table grows — the exact bug this fixes, made invisible.

**Behaviour change to note:** the collector is **on by default**, because
unbounded growth is a bug rather than a tuning preference — a fix every
deployment must opt into fixes nothing for the deployments that do not know they
have the problem. `RetentionConfig::without_rate_limit_bucket_gc()` restores the
pre-#1127 never-collect behaviour, and `RetentionConfig::enabled()` now counts
the GC as an enabling reason so a deployment with every other horizon switched
off still spawns the janitor to honour it.

Tests, red → green → refactor: 24 unit tests across `queue.rs`, `retention.rs`
and `builder.rs` (the family list pinned to the two real key builders and to the
reserved-prefix list, classification of keys produced by
`dynamic_rate_bucket_key`/`throttle::bucket_key` rather than hand-written
literals, LIKE-metacharacter safety, every guard clause present in the rendered
sweep SQL, reuse of the shared effective-value expressions, the
`DELETE ... USING`/`FOR UPDATE SKIP LOCKED` shape, the dry-run preview sharing
the sweep's predicates and being unable to mutate, the ensure statement
rejecting `DO UPDATE` and never writing pacing state or `updated_at`, the touch
interval being shorter than the shortest legal window, both reserved prefixes
rejected as static keys, and the config default/builders/validation — including
that `Some(0)` is rejected rather than treated as "disabled"), and 22 DB
integration tests in
`autumn-harvest/tests/integration/rate_limit_bucket_gc_tests.rs` driving the
real janitor end to end: both families collected, a never-drained bucket
collected once idle, recently-debited and recently-written buckets retained, a
partially drained bucket retained with its pacing state untouched (including the
realistic slow-refill shape), PENDING and RUNNING tasks pinning their buckets
while COMPLETED/FAILED/CANCELLED ones do not, a deferred throttled start pinning
its bucket, a live override retained and a lapsed one collected, an
operator-written baseline retained, bounded static keys never collected, the
window honoured in both directions, `dry_run` forecasting without deleting or
metering, the drain loop converging, the per-tick batch budget spent and the
next tick draining the rest, the collect → re-register → admit-work-again round
trip (with the fail-closed assertion in between), re-ensuring a live bucket
disturbing neither its tokens nor `updated_at`, the throttle path registering
through the same interlocked statement, shard-locality proven against two
genuinely independent shard databases, a failing shard reporting *why* rather
than a silent zero — and two concurrency regression tests that fail without the
interlock: a sweep must collect nothing while an uncommitted enqueue, or an
uncommitted dispatch debit, holds the bucket.
