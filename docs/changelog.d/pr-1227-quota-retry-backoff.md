## Phase — Quota-blocked retries get a bounded backoff (issue #1227)

A sixth automated Codex review round on PR #1221 (issue #946, per-tenant
resource quotas) surfaced four `HarvestError::QuotaExceeded` catch sites that
correctly avoid terminally failing an execution over an unrelated tenant's
transient quota condition, but don't defer the retry with a backoff — so a
target durably at quota cap (e.g. a `max_dead_letters` cap, which only clears
via manual operator action) causes either a zero-delay retry loop or
starvation of unrelated work. Filed as its own issue because #1221 had already
used all 5 permitted Codex review rounds.

**Findings 1 & 2 (P2) — `autumn-harvest/src/worker.rs`.** The local
`QuotaExceeded` catches in `persist_all_started_child_workflows` (the awaited
local-child fan-out path) and `persist_child_timeout_race` (the
`ctx.child_workflow().with_timeout(...)` spawn path) each did
`queue::park_workflow_task` immediately followed by an unconditional
`queue::wake_workflow_task` — exactly the anti-pattern
`recover_from_child_quota_exceeded`'s own doc comment already names and was
built to prevent, but these two sites weren't routed through it. Both now call
that shared helper, which uses `queue::requeue_for_retry` with a bounded
jittered backoff (`QUOTA_RETRY_BACKOFF_MIN`/`MAX`, 500ms-3s) instead.

**A third worker.rs site, found by a post-fix sweep.** A code-review pass
after the initial fix re-audited every `QuotaExceeded` catch in the crate and
found `persist_mixed_suspension_batch` (the heterogeneous "activity × child",
"child × signal", etc. suspension-batch path, issue #950) had the identical
park-then-immediately-wake defect — its own comment called it a "mirror" of
Findings 1 & 2 without that mirroring ever having been implemented. Fixed the
same way. This brings the file's five `QuotaExceeded`-on-child-spawn catch
sites to a single shared recovery path.

**Finding 3 (P2) — `autumn-harvest/src/debounce.rs`.** The quota-blocked fire
retry wrote `effective_fire_at = LEAST(now() + 5s, max_fire_at)`. Once
`max_fire_at` (the row's `max_wait` deadline) had already passed, this always
evaluates to that already-past `max_fire_at`, so the row re-qualified as due
on the very next scanner tick regardless of the intended backoff — defeating
it entirely for exactly the row that most needs it, one stuck past its
deadline on a persistently exhausted quota. Extracted a pure `redefer_target`
function: the clamp still applies while the deadline is still ahead
(preserving the pre-existing `max_wait` contract), but once `max_fire_at` has
passed the backoff applies **unclamped** — the cap has already been blown, so
clamping to it can only produce a stale timestamp, and the alternative
(dropping the row) would silently discard a debounced start the caller is
still waiting on. `FireDueRow`'s claim query now also selects `max_fire_at`
(no extra round trip; it's read under the same `FOR UPDATE` lock already
held), so the decision is made in Rust rather than in a second SQL clamp.

**Finding 4 (P1) — `autumn-harvest/src/completion_trigger.rs`.** The
cross-shard completion-trigger outbox's claim query
(`enforce_completion_triggers_outbox`) had no `ORDER BY` and no per-row
backoff tracking at all; a `QuotaBlocked` relay outcome left the row
completely untouched (neither deleted nor timestamped). With no batch
ordering and no exclusion, a row blocked against a durably exhausted quota
could dominate every unordered `LIMIT 50` claim batch on every scanner tick,
starving any OTHER, unrelated completion-trigger relay that happened to sort
after it. Flagged P1 (the other three are P2) because it starves unrelated
work, not just the blocked execution's own retry. Fixed with a new nullable
`next_attempt_at` column (migration
`20260906014820_harvest_completion_trigger_outbox_backoff`, plus a supporting
`(target_shard, next_attempt_at, created_at)` index): a `QuotaExceeded` catch
inside `relay_gate_checked_start`'s existing claim transaction stamps it to
`now() + 5s`, and the claim query now filters out a row whose backoff hasn't
elapsed. `NULL` (the default, and every pre-existing row) means "never
blocked; eligible immediately", so this is purely additive.

**Finding 4's claim-batch ordering, refined across two more Codex rounds on
PR #1386.** Ordering the eligible batch by `created_at` alone (the fix as it
first shipped) was not enough: once the scanner's `poll_interval` is at or
above the 5s backoff, a persistently-blocked row's backoff has always
re-elapsed by the next tick, so it stays among the 50 oldest eligible rows
forever and a newer, healthy row never gets a turn (**round-1 P1**). Ordering
never-attempted rows (`next_attempt_at IS NULL`) strictly ahead of any
previously-blocked one fixed that, but traded one starvation direction for
the other: a sustained arrival of ≥50 fresh rows between ticks can now fill
every batch and strand a previously-blocked row even after its target's
quota frees up (**round-2 P2**). The batch is now built from independently-
capped queries instead of one combined query + `ORDER BY`: fresh rows get up
to `OUTBOX_CLAIM_BATCH_LIMIT - OUTBOX_RETRY_RESERVED_SLOTS` (40) slots,
retry-eligible rows get the remaining slots with a floor of
`OUTBOX_RETRY_RESERVED_SLOTS` (10) — the fresh query filling short of its own
limit already gives the retry tier the difference (its `retry_limit` grows to
match). The retry tier filling short of ITS limit needed a third query,
added a round later: the common case is no quota-blocked backlog at all, so
without backfilling the leftover reservation with more fresh rows, every
scan was silently capped at 40 of the configured 50 — a permanent 20%
throughput cut for the normal, no-backlog case (**round-3 P2**). Also fixed
alongside round-1: the row-level `FOR UPDATE SKIP LOCKED` claim inside
`relay_gate_checked_start` now re-checks the same eligibility predicate,
closing a race where a concurrent scanner replica's unlocked batch read
could claim and retry a row a peer had just re-armed (**round-1 P2**).

**Tests, red → green → refactor.** Confirmed each fix's test fails without it
(production code reverted, rebuilt, test observed to fail) and passes with it
restored:

- `autumn-harvest/src/debounce.rs`: three new unit tests on the extracted
  `redefer_target` (clamp still applies before the deadline, clamp skipped and
  the target stays in the future once the deadline has passed, the exact
  `max_fire_at == now` boundary).
- `autumn-harvest/tests/integration/quota_enforcement_tests.rs`: new
  assertions on the two existing
  `..._honors_target_quota_parks_parent_then_succeeds` tests (Findings 1 & 2),
  plus a new `mixed_batch_child_spawn_honors_target_quota_parks_parent_then_succeeds`
  test for the third `persist_mixed_suspension_batch` site (an "activity ×
  child" `ctx.race()` composition), proving a completed retry cycle's
  `harvest_task_queue.scheduled_at` lands in the future rather than at/before
  the pre-worker value — the zero-delay hot-spin's exact opposite, sampled
  only once the row settles `PENDING` after moving off its pre-worker
  `scheduled_at` to rule out a read landing mid-claim or before the worker's
  first poll. New test
  `quota_blocked_outbox_row_gets_backoff_and_does_not_starve_a_sibling_row`
  (Finding 4): 60 quota-blocked rows (exceeding the claim query's `LIMIT 50`)
  plus one unrelated free-target row inserted into the outbox — the free row
  is unreachable in the first batch (proving the batch really is dominated),
  then delivered once the backoff filter excludes the blocked rows from a
  later scan; an immediate rescan of a still-backed-off row leaves its
  `next_attempt_at` unchanged (excluded, not reprocessed); forcing the
  backoff into the past and freeing the quota then delivers it on the next
  scan. Two more new tests pin the round-1/round-2 ordering trade-off from
  both directions:
  `quota_blocked_outbox_never_attempted_rows_outrank_expired_quota_retries`
  (round-1 P1) — 60 rows with an already-expired backoff (simulating the next
  scan after a slow poll interval) plus one never-attempted row that is
  OLDER by `created_at` still loses the claim-batch race, proving fresh rows
  outrank expired retries regardless of insertion order — and
  `quota_blocked_outbox_retry_row_is_not_starved_by_a_flood_of_fresh_rows`
  (round-2 P2) — a single retry-eligible row (quota freed, backoff elapsed)
  is still reclaimed in one scan despite 55 competing never-attempted rows,
  proving the reserved retry floor holds under a fresh-row flood — and
  `quota_blocked_outbox_backfills_unused_retry_capacity_with_fresh_rows`
  (round-3 P2) — 45 fresh rows with zero retry-eligible rows competing are
  ALL delivered in one scan, not just the first 40, proving an unneeded
  retry reservation doesn't silently cap normal-case throughput.

No `WorkflowEvent` variant, no data migration, no replay impact.
