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
jittered backoff (`QUOTA_RETRY_BACKOFF_MIN`/`MAX`, 500ms-3s) instead. This
brings the file's four `QuotaExceeded`-on-child-spawn catch sites to a single
shared recovery path.

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
elapsed and orders the eligible batch by `created_at` (FIFO). `NULL` (the
default, and every pre-existing row) means "never blocked; eligible
immediately", so this is purely additive.

**Tests, red → green → refactor.** Confirmed each fix's test fails without it
(production code reverted, rebuilt, test observed to fail) and passes with it
restored:

- `autumn-harvest/src/debounce.rs`: three new unit tests on the extracted
  `redefer_target` (clamp still applies before the deadline, clamp skipped and
  the target stays in the future once the deadline has passed, the exact
  `max_fire_at == now` boundary).
- `autumn-harvest/tests/integration/quota_enforcement_tests.rs`: new
  assertions on the two existing
  `..._honors_target_quota_parks_parent_then_succeeds` tests (Findings 1 & 2)
  proving a completed retry cycle's `harvest_task_queue.scheduled_at` lands in
  the future rather than at/before the pre-worker value — the zero-delay
  hot-spin's exact opposite, sampled only once the row settles `PENDING` after
  moving off its pre-worker `scheduled_at` to rule out a read landing mid-claim
  or before the worker's first poll. New test
  `quota_blocked_outbox_row_gets_backoff_and_does_not_starve_a_sibling_row`
  (Finding 4): a quota-blocked row and an unrelated free-target row inserted
  into the outbox together — the free row is delivered on the very first scan
  regardless of the blocked row sharing the batch; the blocked row is left
  claimable with `next_attempt_at` stamped into the future; an immediate
  second scan leaves that timestamp unchanged (excluded from the claim, not
  reprocessed); forcing the backoff into the past and freeing the quota then
  delivers it on the next scan.

No `WorkflowEvent` variant, no data migration, no replay impact.
