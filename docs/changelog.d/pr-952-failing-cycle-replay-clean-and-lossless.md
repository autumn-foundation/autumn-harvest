## Phase 3.x — Failing decision cycles are replay-clean and lossless (issue #952)

Two documented engine limitations made a workflow's *failing* decision cycle
second-class. Both are closed here, composing only existing events —
**zero new `WorkflowEvent` variants, no migration, no contract change.**

### Seam 1 — a terminal `WorkflowFailed` is transparent to in-progress matches

`HistoryMatcher::new` now computes a **terminal-failure tail**: walking backwards
over pause/resume events (#383) and post-terminal bookkeeping
(`WorkflowRetryScheduled` #523, `ChildWorkflowCascadeApplied` #347), if it lands
on a `WorkflowFailed` that event and everything after it is marked transparent —
the same mechanism #383 and #510 already use. `prepare_match` then answers
`NoMatch` ("the run is failing") where it used to answer `Diverged`.

The rule is *a failed run is verified up to its failure point*: a failing cycle's
history is truncated by construction (the commands it issued never became events)
and the sealed run can never gain another event, so past that point there is
nothing to compare against. Four enforcement points:

1. `HistoryMatcher::new` — transparency (above), plus
   `has_terminal_failure_tail` / `at_terminal_failure_frontier` accessors.
2. `WorkflowContext::strict_replay_no_match_is_divergence` — the shared predicate
   behind both the returning (`check_strict_replay_no_match`) and deferred
   (infallible built-ins) forms — no longer reports an early-completion mismatch
   at the failing frontier.
3. `executor` — the completed-arm "new commands emitted beyond recorded history"
   check is skipped at the failing frontier, so a build that *fixed* the failing
   check and now completes is not reported as drift.
4. `testing::outcome_to_report` — for a failing-tail history, a non-ND `Failed`
   outcome and a `Suspended` outcome both report `ReplaySucceeded`. A handler
   **panic** and any ND-carrying failure still report, as before.

`WorkflowContext::is_replaying()` (and `HistoryMatcher::has_buffered_history`)
gained a matching clause: a sealed-failed run is always "replaying", so every
replay-suppressed side effect (the #379 logger, #532 business metrics,
`set_current_details`, search-attribute patches, progress chunks, `version()`
markers) stays suppressed on a post-mortem query replay of a failed run.

`ReplayReport` gains `reproduced_failure: Option<String>` and a
`failure_message()` accessor, so a replay that reports `ReplaySucceeded` by
reproducing the recorded failure still surfaces the error string (including a
typed `WorkflowFailure` envelope) rather than losing it.

### Seam 2 — `persist_terminal_outcome_commands` no longer drops dispatches

The terminal persist path's hand-maintained allowlist is replaced by
`terminal_command_policy`, an **exhaustive** `match` over every
`WorkflowCommand` variant returning a documented `TerminalCommandPolicy`
(`PreTerminalEvent` / `SidePath` / `AbandonedDispatch` / `NoRecord(reason)` /
`Unreachable(reason)`). A new command variant is now a compile error until
someone states what a terminal cycle does with it (AC4's asserted invariant),
and `every_workflow_command_variant_is_classified` pins all 26 classifications.

`AbandonedDispatch` covers the two kinds that would otherwise have become work
other subsystems can see — `StartChildWorkflow` (a child execution row) and
`ScheduleActivity` (a task-queue row). A **failing** cycle now records each at
its own command-emission position as the `*Started`/`*Scheduled` event the
suspension path would have written, immediately followed by a synthetic terminal
carrying `event::ABANDONED_DISPATCH_REASON` — issue #952's option (ii). The
terminal is what makes the record replay-*resolvable*: without it the branch
would re-park forever on work that can never arrive.

Scope guards:

* **Failed outcomes only.** `Completed` / `ContinuedAsNew` keep today's exact
  behaviour: a completed history is verified in full by strict replay, so a
  synthetic terminal there could resolve a branch the live cycle left parked and
  flip a `select!` on the next replay of a healthy run.
* **Fresh dispatches only.** A `StartChildWorkflow` whose `child_id` already has
  an execution row is `spawn_child_workflow_raw`'s `ChildInProgress` re-park, not
  a new dispatch; it is skipped via the same row check the suspension path uses.
  `ScheduleActivity` is fresh by construction (a re-park emits `WaitForActivity`).
* **No metrics.** Nothing was enqueued and nothing ran, so no activity/child
  failure counter is incremented for work that never existed.
* The history hard-cap preflight counts these events (conservatively, before
  dedup), so a near-cap failing batch dead-letters instead of breaching the cap.

### Behaviour change for existing callers

A `WorkflowReplayer` replay of a history ending in `WorkflowFailed` that
reproduces the failure now reports `ReplaySucceeded` (with
`reproduced_failure` set) instead of `ReplayStatus::WorkflowFailed`. Stored JSON
is untouched: no event is rewritten, re-tagged, or reinterpreted — pre-fix
histories (with dropped commands) replay through exactly the same matcher, and
now simply reproduce their recorded failure instead of false-flagging drift.

Two knock-on effects of that verdict change, both intended:

* `harvest_replay --fail-on-error` exits `0` where a failed-run fixture used to
  exit `1`. The flag means "this history revealed a problem with the current
  build", and a reproduced failure no longer is one. `backup_verify` is
  unaffected: its sampler selects only `RUNNING`/`PAUSED`/`SUSPENDED` rows, so no
  sampled history can carry a failing tail.
* `ReplayReport` gains a public `reproduced_failure` field. The struct is not
  `#[non_exhaustive]`, so an out-of-tree struct-literal construction of it needs
  the new field — permitted under the repo's 0.x semver convention, and the
  in-tree readers all go through the new `failure_message()` accessor instead.

### Codex review round 3

* **P2 — the history hard-cap preflight counts what will actually be appended.**
  The `Failed` arm added `+2` events for every pending dispatch command, including
  ones `AbandonedDispatchPlan` then drops (a child already backed by an execution
  row, a dispatch already in history). Over-counting is *not* the safe direction
  here as it is for `timer_lifecycle_event_count`: breaching the cap dead-letters
  the execution and replaces its real failure with a history-cap error, so a
  workflow re-parking many already-started children could be terminally
  mis-routed on events that were never going to be written. The preflight now
  resolves the same dedup persistence applies
  (`abandoned_dispatch_event_count_resolved`), and
  `the_cap_preflight_counts_only_the_dispatches_that_will_be_written` pins the
  count against the events `terminal_pre_outcome_events_from_commands` actually
  emits.

### Codex review round 2

* **P2 — the abandoned-*activity* terminal is matched by its full engine shape.**
  The child arm already required the exact shape the engine writes, because a
  child's `error` is the child author's own string; an activity's `error` is the
  activity author's own string in exactly the same way, and that arm was matching
  on the reason text alone. A genuine `ActivityFailed` quoting
  `ABANDONED_DISPATCH_REASON` would therefore have been classified synthetic, and
  a redriven run would have re-dispatched the activity — repeating its side
  effects — instead of replaying its recorded failure. The arm now destructures
  `attempt: 1`, `error_type: "Error"`, `non_retryable: true`, `details: None`
  alongside the reason (`a_genuine_activity_failure_quoting_the_reason_is_not_an_abandoned_record`).

### Codex review round 1

* **P1 — abandoned-dispatch transparency is bounded by the last redrive.** The
  #510 rule marked *every* abandoned-dispatch pair in a redriven history
  transparent. A run that was redriven and then failed AGAIN wrote fresh pairs
  after that redrive: those belong to the terminal-failure tail's own cycle, so
  making them transparent left the latest failing cycle's child/activity looking
  like an unresolved fresh dispatch — the replay parked instead of re-deriving
  the recorded failure path, and still reported a clean verdict. `HistoryMatcher::new`
  now scans only the prefix before the last `WorkflowRedriven`
  (`abandoned_records_written_after_the_last_redrive_stay_opaque`).

### Review round 1

Four review passes (replay semantics, worker persistence, API/docs, test quality)
ran against the finished implementation; every finding is fixed in this change.
The load-bearing ones:

* **A park is only absorbed at the failing frontier.** `run_strict_with_ctx`'s
  suspend arm now runs the canary path's `history_has_unconsumed_events()` check,
  so a build that parks EARLY on a failing-tail history still reports drift.
* **A wrapped non-determinism is never absorbed.** The `Failed` absorption is
  gated on `non_deterministic_details.is_none()` rather than on the error
  string's `"non-deterministic replay: "` prefix, which a workflow that wraps the
  engine error loses.
* **DLQ redrive (#510) is not poisoned.** A redriven history's abandoned-dispatch
  pairs are marked transparent, so the reopened cycle re-issues the dispatch live
  exactly as it did pre-#952 — the audit record stays, the replay does not read
  it back. The redrive backward scan also learned to skip post-terminal
  bookkeeping, fixing a retried-then-redriven run's superseded terminal.
* **The dedup asks the matcher's question.** `RecordedDispatchIds::from_history`
  is consulted alongside the execution-row check (they disagree after a retention
  sweep or a partial restore), and it covers `ScheduleActivity` too.
* **One replay-suppression predicate.** `replay_suppresses_side_effects()` backs
  `is_replaying()` and the guards that lock the matcher directly (#379/#790
  logger, #791 progress, `info().is_replaying`); `version()` uses the cursor-only
  `at_history_frontier()`, the same question `match_version` asked.
* **`next_event_id` is re-read under the row lock** the persist transaction
  already holds, instead of using the pre-handler snapshot — a child terminal
  committing in between would otherwise collide on
  `UNIQUE(workflow_exec_id, event_id)` and roll this cycle's race-loser
  cancellations back with it.
* **A dropped dispatch is never silent**, even on the `Completed`/`ContinuedAsNew`
  path this issue deliberately leaves unchanged: one `tracing::warn!` per cycle.
* **The plugin's replay-diagnosis endpoint** surfaces the reproduced failure
  instead of answering `clean` with `failure: null`.

Documented rather than changed: a failing replay that returns `Err` without
reaching every recorded event is not reported — a fail-fast fan-out is exactly
that shape, and the recorded run failed for the same reason (see the table in
`docs/replay-verify.md`).

### Tests (TDD red → green → refactor)

* `replay.rs`: 7 matcher tests — trailing `WorkflowFailed` transparent; an
  in-progress match at the frontier is `NoMatch` not `Diverged`; the tail spans
  post-terminal bookkeeping; a `WorkflowFailed` with real history after it is not
  a tail; completed/cancelled tails are not failure tails; a redrive tail is not a
  failure tail (#510's rule unchanged); the frontier requires every pre-terminal
  event consumed.
* `replayer_tests.rs`: the two known-limitation tests flip to positive
  `ReplaySucceeded` assertions
  (`early_config_dependent_failure_replays_cleanly`,
  `replayer_replays_a_payload_cap_fan_out_failure_cleanly`), plus a
  6-shape failure-history corpus (early config-dependent failure; fan-out
  payload cap; failed-before-suspend with 1 and with 3 dispatched children;
  failure after a marker; failure after a side effect) and two negative
  controls (drift *before* the failure point, and a completed history with an
  extra command, both still `NonDeterminismDetected`).
* `worker.rs`: the 26-variant policy table; abandoned child/activity record
  shape; the re-park dedup; a non-failing terminal cycle records none; emission
  ordering; the cap-preflight bound; and the success metric end-to-end without a
  database — the real workflow through the real executor, its real drained
  commands through the real terminal event planner, asserting all three
  dispatched children appear, plus a replay of that exact history reporting
  `ReplaySucceeded`.
