# Snag — exploratory QA log

Session log and charter queue for the "Snag" exploratory QA agent (see the
agent's own operating charter for process, oracles, and the filing bar). Each
session should:

1. Read **Next charters** below and pick from it first (falling back to fresh
   churn in `git log` / `docs/changelog.d/` when the queue is empty).
2. Append a **Session log** entry when done, even — especially — when no bugs
   were found.
3. Replace **Next charters** with whatever follow-ups the session surfaced.

This file is the only durable handoff between sessions: there is no other
memory across runs, so a session that doesn't update it loses its own
follow-ups.

## Next charters

- **Scheduler fire-claim race (issue #350/#771 "409 while a fire claim is
  live")**: `PATCH /admin/schedules/{id}` claims to return `409` when a
  scheduler replica holds a live fire claim on the row, fence an expired
  claim's token, and guarantee a fire landing concurrently with an edit
  always dispatches the committed new spec. Not driven live this session —
  needs two connections racing a tick against a PATCH with sub-second timing
  control (e.g. hold a transaction open on the claim UPDATE, or a unit/DB
  integration test harness rather than curl). Look at
  `apply_workflow_schedule_update`'s `guard_live_fire_claim` path and the
  `#771 AC7` tests in `autumn-harvest/src/scheduler.rs`.
- **Backfill calendar-rebase collision guard**: `schedule_backfill_inner`
  keeps `(original_slot, fire_time)` pairs specifically so that two distinct
  cron slots rebased onto the same business day by
  `RunNextBusinessDay`/`RunPrevBusinessDay` don't collide on the derived
  workflow ID. Not driven live — construct a calendar with adjacent
  exclusions that rebase two slots onto one day and backfill across them,
  checking for a dropped run or a duplicate-ID conflict.
- **`POST /admin/schedules/{id}/trigger` vs. `max_active_runs`/buffer overlap
  policies under rapid double-submit**: the interrupt tour ("submit twice
  fast") was not run against schedule trigger/backfill endpoints this
  session, only against preview/create/PATCH.

## Session log

### 2026-09-03 — Charter: operator managing workflow schedules (API + Vantage)

Time: ~2h. Driven live against a real `cargo dev` instance (Postgres 16,
locally started) — API only, browser/Vantage UI not exercised beyond
reading its query-string-escaping code.

**Toured, solid** (boundary/data/interrupt tours run, no bug found):
- `POST /admin/schedules/preview` — malformed/empty cron, negative/huge
  `count`, garbage IANA timezones, impossible calendar dates (Feb 30, Apr
  31), DST spring-forward/fall-back gaps (America/New_York), `interval:`
  zero/negative. All 400 or correctly truncated; the DST-gap skip-to-next-
  occurrence behavior is deliberate and covered by
  `next_run_after_spring_forward_skips_nonexistent_local_time`.
- `max_runs=0` on create/PATCH — looked like a silent "budget of zero ⇒
  unlimited" bug at first read of the JSON response, but it's a documented,
  tested, codebase-wide convention (`docs/api-contract.json`: "explicit null
  (or 0) = remove the budget"; `schedule_remaining_runs`'s `max_runs > 0`
  convention). Not a bug — ruled out before filing.
- PATCH idempotency claim ("re-applying the same partial update converges on
  the same row state") — held under repeated identical PATCHes, both cron
  and interval schedules, several seconds apart.
- Upsert's claimed `is_paused` preservation on re-registration — verified in
  code (`apply_workflow_schedule_update`: "is_paused is deliberately
  excluded").
- Reflected-XSS hypothesis in Vantage's `PreEscaped(&base)` query-string
  builders (`ui.rs`) — ruled out; `url_encode` is a strict allowlist encoder.
- Backfill date-range/count bounds — explicit `to < from` rejection,
  `DEFAULT_BACKFILL_MAX_COUNT` cap via `LimitExceeded`.

**Findings:** none met the filing bar (minimal repro + named oracle + rate +
pinned environment). The schedule/retention code in this repo is unusually
well-fortified — explicit multi-round review comments ("Codex review round
N"), tests matching the documented behavior closely, and public API-contract
docs that track implementation precisely.

**Next charters:** see above.
