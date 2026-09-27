## Phase 5.x — stall diagnosis: timer provenance survives a scheduled_at drift (issue #1402)

Follow-up to issue #1191 (PR #1403): `stall_diagnosis::is_the_missed_timer_wake`
trusted an exact or near-exact match between a workflow task's `scheduled_at`
and a durable timer's `fires_at` as proof the row is that timer's own missed
wake. Three production paths can move `scheduled_at` an *unbounded* distance
from `fires_at` for the identical wake reason — a queue-pause resume credit
(`queue_pause::resume_shift_scheduled_at_query`), a poison-pill orphan reclaim,
and a capability-miss release — so a genuinely stalled, timer-owned run could
fall through the exact/near match and report the healthy-looking
`sleeping_timer` instead of `timer_overdue`. Confirmed independently by Codex's
round-5 review of PR #1403, with a concrete repro path: pause a timer-owned
workflow queue across its deadline, resume it, and let dispatch stay
saturated.

**Fix: a persisted provenance marker, not a wider heuristic.** A tighter
timestamp tolerance would only relocate the coincidence window this class of
bug already exploits; a looser one risks misattributing an unrelated repend to
a stale timer. Neither actually distinguishes "the same wake reason, just
delayed" from "a different wake reason that happens to land nearby". New
nullable column `harvest_task_queue.timer_fires_at`
(`20260921011505_harvest_task_queue_timer_fires_at`), set only by
`queue::reschedule_task` from the identical value it writes to `scheduled_at`.
Every other write path either preserves the same wake reason (so keeping a
stale marker is correct, not stale) or already stamps `created_at` fresh
(`primary_repend_workflow_task_query`, `release_suspended_workflow_claim_query`,
both now also clear the marker explicitly) — except
`shard_rebalance::activate_target`'s signal re-pend, which previously did
neither and so risked the *opposite* failure (misattributing a signal-driven
repend to a stale unfired timer); it now stamps `created_at` and clears the
marker in the same statement, bringing it into the set
`wake_source_repended_this_row` already recognizes.

`is_the_missed_timer_wake` gained a third, additive match:
`task.timer_fires_at == Some(timer.fires_at)`, gated by the same
`wake_source_repended_this_row` veto the existing close-match tolerance
already uses — so a future write-path bug that forgets to clear the marker
fails safe (a missed `timer_overdue`, never a false one).

**Review round.** Three independent agent reviews (correctness, test
coverage, docs) found the first cut's writer/clearer enumeration was not
exhaustive: `queue`'s three backoff-retry paths
(`requeue_workflow_task_for_quota_retry`, `requeue_workflow_task_nd_blocked`,
`requeue_workflow_task_after_panic`) also move a workflow task's
`scheduled_at` for a non-timer reason, without touching `timer_fires_at` or
`created_at`. Traced every call site: none is currently reachable with a
still-armed timer, because `ingest_due_timers_and_signals` always consumes a
due timer before any of the three can run. Fixed anyway, defensively, since
that safety is an unenforced ordering invariant in `worker.rs` rather than
something these three functions themselves guarantee — a future reordering
could silently reopen the gap. All three now clear `timer_fires_at` in the
same `SET` clause, and the doc comments (`WorkflowTaskFacts::created_at`/
`timer_fires_at`, this migration's upgrade-guide row) now name all the
writers and clearers instead of claiming an incomplete "only two". New
shape tests pin that `poison_pill::requeue_orphan_stmt` and
`queue::release_task_for_capability_miss_query` (which must PRESERVE the
marker) never mention `timer_fires_at`.

**No new `WorkflowEvent` variant, no replay/determinism impact.** The column
is a live side-table marker read only by the diagnose endpoint's pure
classifier; nothing in the replay path touches it.

**Tests.** True red/green TDD throughout:
- Unit (`stall_diagnosis.rs`, no DB): 3 new tests model the resume-shift
  shape directly — correlation via the preserved marker after an unbounded
  drift, the marker correctly vetoed by real repend evidence, and the
  pre-#1402 fallback behavior for a row with no marker. All 145 tests in the
  module pass.
- SQL shape tests (`queue.rs`, no DB): `reschedule_task` stamps
  `timer_fires_at`; the two `created_at`-stamping repend queries clear it.
- DB integration (`autumn-harvest-plugin/tests/stall_diagnosis_integration.rs`):
  `overdue_timer_still_wins_after_a_queue_pause_resume_shift` reproduces
  Codex's exact regression end to end against real Postgres — arms a timer via
  the real `queue::reschedule_task`, pauses and resumes the queue via the real
  `queue_pause::pause_queue`/`resume_queue`, and asserts `timer_overdue`.
  Confirmed RED against the pre-fix classifier (reports `sleeping_timer`/
  `healthy`) before the fix, GREEN after. A second test,
  `queue_pause_resume_does_not_misattribute_an_unrelated_timer_to_a_signal_repend`,
  pins the issue's original ask: an unrelated timer near a signal-repended row
  stays safe through a resume shift.
- DB integration (`autumn-harvest/tests/integration/shard_rebalance_db_tests.rs`):
  `activation_repend_does_not_misattribute_a_stale_timer_to_the_new_signal_wake`
  drives a real two-shard migration of a timer-armed execution through
  `activate_target`'s signal re-pend and confirms both the row's own
  provenance columns and `stall_diagnosis::classify_execution`'s verdict.
  Confirmed RED against the pre-fix `activate_target` SQL, GREEN after.

`cargo test -p autumn-harvest --no-default-features` (1365 tests) and
`cargo test -p autumn-harvest --features db --lib` (3623 tests) are green with
zero regressions; `cargo test -p autumn-harvest-plugin --lib` (1273 tests) is
green. `docs/upgrading/0.5.0.md` and `docs/rnd/sqlite-feasibility.md`'s
migration count are updated to match.

**Second review round (Codex).** Two more findings, both addressed without
weakening the veto above.

First: a row already armed by `reschedule_task` before this migration ran
got `NULL` for the marker, not because it lacked a timer, but because the
column did not exist yet. `is_the_missed_timer_wake`'s exact-match branch
covered it right up until the first post-upgrade drift, then lost it —
reopening this same issue for that one pre-existing row. The migration now
also backfills `timer_fires_at` for the unambiguous case: a `PENDING`/
`RUNNING` workflow row whose `scheduled_at` exactly matches a still-unfired
timer on the same execution. `docs/upgrading/0.5.0.md`'s blanket "no data
backfill" claim is corrected to name this one exception.

Second: a short-lived draft tried exempting the marker match from the
`wake_source_repended_this_row` veto entirely, on the reasoning that only
`reschedule_task` ever writes it. That is true only of code that knows the
column exists. During this very migration's own rolling deploy, an
old-binary worker's repend for a genuinely different wake reason clears
nothing it has never heard of, and an unconditionally trusted marker would
then report a false `timer_overdue` on a row that is not actually stalled
— a near-guaranteed failure on every rollout of this fix, not a
hypothetical one. Reverted; the veto stays as originally shipped. The
narrower cost that draft was chasing — a genuinely short timer's own
`created_at`-to-`fires_at` gap landing inside the veto's slack after a
same-reason drift — is accepted as the lesser failure mode and is now
pinned by its own test.

**CI-red root cause (this PR's own bug, not infra flake).** `chain_timeout_tests`,
`workflow_retry_tests`, `rate_limit_key_tests`, and other suites that build
their Postgres container from `integration_e2e.rs`'s hand-rolled `INIT_SQL`
bundle failed intermittently across shards — `column "timer_fires_at" of
relation "harvest_task_queue" does not exist` on one test, and worker-driven
timeouts on the rest, because `reschedule_task`'s changeset and the repend
queries that clear the marker all name the column unconditionally. This is
the exact same omission class as issues #1685/#1596/#1317, already
documented in that file: a new migration's columns are invisible to a
throwaway testcontainers database until its `include_str!` is added to
`INIT_SQL` by hand, and a local run against an already-migrated
`HARVEST_TEST_DATABASE_URL` database never surfaces the gap. Fixed by adding
`20260921011505_harvest_task_queue_timer_fires_at` to the bundle. Verified
by concatenating `INIT_SQL`'s own file list exactly as the Rust `concat!`
does, applying it to a fresh database, and confirming both a clean apply and
the column's presence — then re-running the previously-failing
`chain_timeout_tests::pause_resume_shifts_run_deadline_but_not_chain_deadline`
against that database.
