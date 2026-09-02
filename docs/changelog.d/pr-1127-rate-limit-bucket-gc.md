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
by how long finished histories are kept — and is suppressed entirely under
`dry_run`, which unlike partition *creation* has nothing to preserve here.

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
  created_at)`, so a debit, an operator override write, or a bucket registered
  but never used all count as activity.
- **Full.** Effective available tokens at or above the effective burst, derived
  from the *same* `effective_available_tokens_expr` / `effective_burst_expr` the
  debit path uses. Deleting a partially drained bucket would hand out free
  capacity, because re-registration resets `tokens = burst`; with this, delete +
  re-register is **token-neutral by construction** rather than by timing luck.
- **No live TTL'd override** (#945) — a delete would silently destroy an
  operator's incident mitigation.
- **No live dependent.** Anti-joins against non-terminal `harvest_task_queue`
  rows and against `harvest_start_throttle`. Both the claim-time gate and
  `try_consume_rate_limit_token` fail **closed** on a missing bucket, and
  nothing re-registers a bucket for an already-enqueued task or an
  already-deferred start, so collecting one of those strands the work forever.
  The task predicate is `NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')` so a
  future task state fails safe toward *retaining*.

**The concurrency interlock (the P1 this design's own reverse-brainstorm
surfaced).** The anti-joins cannot see an enqueue whose transaction had not
committed when the sweep took its snapshot — that bucket would be collected out
from under a task about to reference it, and stranded permanently. Closed by
making `ensure_rate_limit_bucket` an `ON CONFLICT (key) DO UPDATE SET updated_at
= NOW() WHERE <row is stale>`: `DO NOTHING` takes no lock on the conflicting
row, `DO UPDATE` does. The sweep selects `FOR UPDATE SKIP LOCKED`, so whichever
runs second sees the other's outcome — the sweep skips a locked row, and an
ensure blocked behind a delete simply re-inserts the bucket. The `WHERE` keeps
it off the hot path: a bucket touched within
`RATE_LIMIT_BUCKET_TOUCH_INTERVAL_SECS` (5 min) is written **zero** times, and a
bucket stale enough for the GC to consider is by definition older than that,
since the configured window has a 1-hour floor. `throttle::ensure_throttle_bucket`
now delegates to that one function instead of carrying a second copy of the
statement, which would have silently missed the interlock.

**Bounded work per tick.** Up to `MAX_RATE_LIMIT_SWEEP_BATCHES_PER_TICK` (50)
`batch_size` batches per shard, mirroring the partition sweeper's drop budget,
so the first sweep of a table that grew for months cannot turn one janitor tick
into an open-ended delete loop; successive ticks converge.

**Observability.** `RetentionTickResult.rate_limit_buckets_deleted` (per shard,
via `GET /admin/retention`, whose contract entry is already `free_form`) plus
the counter `harvest.retention.rate_limit_buckets_deleted`, labeled by the
bounded `family` and **never** by the bucket key — labelling by the key would
trade an unbounded table for an unbounded metric series, the same call made for
the per-key gauges (#699) and `harvest.codec.reencrypted` (#948). New dashboard
panel and rows in `docs/telemetry.md` and ADR-0001 §7.

**One migration** (`20260902133132_harvest_rate_limit_bucket_gc`): a single
partial index, `idx_harvest_task_queue_rate_limit_key_live`, backing the
non-terminal-task anti-join. The pre-existing
`idx_harvest_task_queue_rate_limit_key` is partial on `state = 'PENDING'` and
cannot serve it — a RUNNING task (a circuit-breaker activity debits at dispatch,
not at claim) and a retry-bound one pin their buckets just as hard. No column
change, no data migration, **no `WorkflowEvent` variant**, and nothing writes to
`harvest_events` — the invariant list in `CLAUDE.md` gains no fourth exception.

**Behaviour change to note:** the collector is **on by default**, because
unbounded growth is a bug rather than a tuning preference — a fix every
deployment must opt into fixes nothing for the deployments that do not know they
have the problem. `RetentionConfig::without_rate_limit_bucket_gc()` restores the
pre-#1127 never-collect behaviour, and `RetentionConfig::enabled()` now counts
the GC as an enabling reason so a deployment with every other horizon switched
off still spawns the janitor to honour it.

Tests, red → green → refactor: 13 new `queue.rs`/`retention.rs` unit tests (the
family list and its agreement with the gauge sampler's exclusion filter, family
classification including near-miss prefixes, every guard clause present in the
rendered sweep SQL, reuse of the shared effective-value expressions, the
batching/`FOR UPDATE SKIP LOCKED`/`DELETE`-first shape, the ensure/touch
statement never writing pacing state, the touch interval being shorter than the
shortest legal window, and the config default/builders/validation/`enabled()`),
and 14 DB integration tests in
`autumn-harvest/tests/integration/rate_limit_bucket_gc_tests.rs` driving the
real janitor end to end: both families collected, a never-drained bucket
collected once idle, recently-debited and recently-`updated_at` buckets retained,
a partially drained bucket retained with its pacing state untouched, PENDING and
RUNNING tasks pinning their buckets while COMPLETED/FAILED/CANCELLED ones do
not, a deferred throttled start pinning its bucket, a live override retained and
a lapsed one collected, bounded static keys never collected, the window honoured
in both directions, `dry_run` collecting nothing, the drain loop converging, the
collect → re-register → admit-work-again round trip (with the fail-closed
assertion in between), the stale-bucket touch keeping the sweep off a
just-ensured bucket while a fresh one is not written at all, and shard-locality
proven against two genuinely independent shard databases.
