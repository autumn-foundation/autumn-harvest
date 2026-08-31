## Phase 6.1 — Mixed-kind concurrent waits in one suspension batch (issue #950)

`futures::join!`/`try_join!` and `ctx.race()` now accept **any combination** of
ctx-managed awaitables — activities (fresh dispatch or re-park), durable timers,
signal waits and child workflows — in a single call. Hedging a slow activity
against a deadline, awaiting a child under a timeout, and listening for an abort
signal while an activity runs are expressible directly instead of being runtime
failures.

**What was wrong.** The worker's suspension dispatcher
(`handle_suspended_workflow`) pattern-matched a closed set of *homogeneous*
batch shapes: N `ScheduleActivity`, N `WaitForActivity`, N `StartChildWorkflow`,
one `StartTimer`, the #476 timer+signal pair, the #779 `__child_timeout:`
child+timer race, N `WaitForSignal`, one external activity, one solo mutex
acquire. Everything else fell through to `"workflow task suspended with
unsupported commands …; this command set is not implemented yet"` and terminally
failed the workflow. `ctx.race()` rejected mixed branch kinds up-front with
`HarvestError::Config` precisely *because* the worker could not persist the
batch.

**What shipped.**

- `extract_mixed_suspension_batch` / `persist_mixed_suspension_batch`
  (`worker.rs`) — classify an arbitrary batch into per-kind buckets and persist
  activity enqueues, durable timer rows, signal waits and child starts in **one
  transaction**. Wired as the **last** arm of the dispatch chain, immediately
  before the fail-loud path, so every shape with its own dedicated persist
  function keeps it and existing histories are byte-for-byte unchanged. The
  extractor's `match` is exhaustive with an explicit reject arm (no
  `_ => {}` catch-all), so `RunLocalActivity`, `AcquireMutex`,
  `ScheduleExternalActivity` and any future command kind still fail loud rather
  than being silently dropped.
- Reuses the per-kind primitives rather than duplicating them. The activity
  dispatch resolution (`build_activity_enqueue_plan`) and the broken-worker-
  session sweep (`fail_activities_for_broken_sessions`) are extracted verbatim
  from `persist_scheduled_activities` and shared, so queue/retry/timeout
  defaults, the #620 builder floor, session pinning, concurrency and rate-limit
  keys, quota admission and producer spans cannot drift between the two paths.
- **Park semantics.** One boolean decides the shape: does the batch arm a
  deadline? If yes, `reschedule_task` to the **earliest** deadline plus the
  `mixed_signal_suspension` sentinel, so any *other* branch's wake pulls the
  `PENDING` row forward instead of sleeping to the deadline; if no, an ordinary
  `park_workflow_task`. Both honour the `wake_requested` no-lost-wake machinery
  — the park reads-and-clears it inherently, and the reschedule sub-path does so
  explicitly via the new `queue::take_wake_requested` (`reschedule_task` alone
  never touched the flag). Post-commit, pending signals, waited-activity
  terminals and terminal children are each re-checked and a self-wake issued,
  mirroring the single-shape paths this generalizes. The re-checks are
  deliberately non-locking reads *after* the transaction, so no child row lock is
  ever taken while the parent's is held (which would invert the
  child-completion path's own lock order).
- **`ctx.race()` accepts mixed branch kinds.** A timer branch arms a durable
  timer under the reserved deterministic id `__race:{seq}:{index}` and, when it
  loses, that still-armed row is deleted by the same `CancelRaceLosers` teardown
  that cancels a losing activity or child; a losing signal branch needs no
  teardown. The #600 winner-marker contract is unchanged. The exactly-one-timer
  + exactly-one-signal pair keeps its dedicated pre-#950 implementation (the
  `__signal_timeout:{seq}:{name}` id, no `race:{seq}` marker, fixed role-based
  indices), so histories recorded before this change replay unchanged.
- `HistoryMatcher::match_race_signal` — `match_signal` with the
  stray-interleaved-event divergence relaxed for a race branch only. A race
  branch's signal that never arrived is the normal "this branch lost" outcome,
  and its siblings legitimately record their own events around it; neither is a
  divergence. The round-13 park-forever protection (issue #768) still holds —
  see the round-3 note below for where it is now enforced. The scan is
  **bounded by the race's own `race_winner:{seq}`
  marker**, so a *losing* signal branch can never consume a `SignalReceived`
  delivered after the race resolved — that signal belongs to a later
  `ctx.wait_for_signal`, which would otherwise park on a signal already
  delivered.
- `ctx.race()` **holds the signal pump across its signal phase** so a push
  handler registered for a branch's name cannot claim the signal that branch is
  there to resolve on. See the round-7 section below for why the ordering this
  originally shipped with was not enough. Signal branches are still evaluated
  first, but purely as ordering hygiene; it is order-independent either way,
  because the branch INDEX decides the tie-break.
- Concurrently-armed race deadlines share one virtual-clock anchor, so a
  resolved timer branch advances `ctx.now()` to `anchor + duration` (a monotonic
  max), never by summing durations — matching what `TestRunOutcome::final_now`
  computes from the same history, and mirroring #768's `advance_timer_clock_to`
  rationale.
- The fan-out "stale re-park command leaks into the next suspension batch"
  limitation (#359/#601) no longer terminates a workflow: the mixed batch is
  persistable, the still-running child is recognised as already-started, and the
  pinning tests in `tests/integration/child_fanout_tests.rs` are now positive
  assertions.
- **AC8** — a `RunLocalActivity` co-batched with a durable sibling wait is
  rejected with an immediate typed `HarvestError::Config` naming the conflicting
  commands. Before this, `extract_run_local_activity` silently ignored the
  siblings, so `join!(ctx.local_activity(..), ctx.timer(..))` dropped the
  `StartTimer` and armed the deadline a whole decision cycle late with no error
  anywhere.

**Pre-existing lost-wake bug fixed on the way (found by review).**
`queue::reschedule_task` never touches `wake_requested`, so the two other
reschedule-based parks — `persist_started_timer` (#476) and
`persist_child_timeout_race` (#779) — silently discarded any wake that landed on
the still-claimed row before their transaction took its lock: an update admitted
by `execute_update_in_process`, a mutex head-of-line grant, a session break —
every waker not serialized behind the execution's row lock. Each park's own
post-commit re-check covers only its own branch kind (pending signals, or the
child's terminal), so the workflow slept to its deadline with the wake already
delivered and then zeroed by the next `claim_task`. Both now read-and-clear the
flag via the new `queue::take_wake_requested` and self-wake, as the generalized
path does. This mattered here because #950 newly routes traffic onto
`persist_started_timer`: `race().timer(..).signal(a).signal(b)` emits one
`StartTimer` plus signal waits, a shape `extract_started_timer_for_suspension`
claims.

**Full mixed-batch parity for the plain signal wait (Codex round 1, P1).**
`match_signal` — the matcher a plain `ctx.wait_for_signal` uses, distinct from
the race path's `match_race_signal` — tolerated only a TIMER sibling; issue
#1071 scoped the rest out because no batch could pair a signal wait with an
activity or a child. This PR makes exactly that batch persistable, so the gap
became reachable: `join!(ctx.wait_for_signal("go"), ctx.execute_activity(..))`
persisted correctly and then **nd-blocked on its first wake**, because the batch
records only the sibling's `ActivityScheduled` and the signal wait — polled
first by `join!` — treated that recorded event as a divergence. The interleaved
sets now mirror `match_timer_strict` / `scan_activity_terminal`: sibling
commands (`ActivityScheduled`, `ChildWorkflowStarted`, `LocalActivityScheduled`,
markers, side-effects) are crossed as interleaved commands with rewind, and
their progress/terminal events are crossed transparently.

The strict/canary replay frontier test needed the same widening
(`check_strict_replay_signal_no_match`): "the cursor is at end of history" is
the wrong question for a signal wait in a mixed batch, because the sibling's
events are legitimately unconsumed ahead of it. The deploy canary would have
reported a healthy in-flight park as a false non-determinism and blocked
deploys. The frontier question that actually holds is whether any matching
signal remains available at all — mirroring the #779 fix, which exempted the
child-timeout race's `InProgress` arm for the same reason.

**The signal scan stops guessing about crossed commands (Codex round 3, P1).**
Round 1 left the TIMER sibling on a different rule from the activity/child
sibling: a crossed `TimerStarted` still flagged the round-13 park-forever hazard
(issue #768) and diverged at end of scan. That rule is only correct when no
supported batch can put a timer of the CURRENT decision at that position — and
this PR is what makes one:
`join!(ctx.wait_for_signal("go"), ctx.timer("t", 30), ctx.execute_activity(..))`
persists `TimerStarted` + `ActivityScheduled`, then nd-blocks on its first wake,
because `join!` polls the signal first and its scan crosses the timer the very
next branch is about to claim. An advertised AC1 composition, broken immediately
after being made persistable.

The two histories — a sibling timer of this decision, and a genuinely stray one
left by code that no longer runs — are **not distinguishable at scan time**, and
cannot be: the scan runs before the sibling branches are polled, so the
information that separates them (does anything in this decision claim the event?)
does not exist yet. The signal scan is a lookahead, and a lookahead must not
return a verdict on events it merely passed over. So the timer/cancel/detached
variants now join the activity/child arm under one rule — cross
non-consumingly, park — and the verdict moves to the end of the cycle, where
`executor.rs` already fails the workflow when `history_has_unconsumed_events()`.
A stray command is still unclaimed there; a mixed batch's is not. This is the
choice already made for `wait_for_signal_timeout`'s strict-`Suspended` arm, for
the same reason.

Round 13's protection is preserved in **outcome**, not in mechanism, and the
existing end-to-end pin
(`interleaved_sibling_signal_stray_timer_started_still_diverges`) still asserts
the replay is reported as non-determinism.

**Behaviour change worth noting:** `ctx.wait_for_signal` no longer returns
`HarvestError::NonDeterministic` *synchronously* for a history carrying a stray
unconsumed timer — it parks, and the run is nd-blocked at the end of the
decision cycle instead. The workflow outcome is identical; only the moment of
detection moves. Code that drives a `WorkflowContext` directly (rather than
through the executor) and expected that synchronous error will now see the wait
park, so it needs the executor's cycle boundary — or an explicit
`history_has_unconsumed_events()` check — to observe the divergence.

**The AC8 conflict list was incomplete (Codex round 4, P2).** The rejection
listed the six obvious durable awaitables and missed two that also reach the
local-activity dispatch arm:

- `ArmTimer { for_await: true }` — what `TimerHandle::await_fire()` emits. It is
  the one and only arm that inserts the `harvest_timers` row and makes a
  cancellable timer fire-eligible, and it parks, so it is a durable await rather
  than bookkeeping. Unlisted, `join!(ctx.local_activity(..), handle.await_fire())`
  entered the local-activity path and `extract_run_local_activity`'s `_ => {}`
  catch-all dropped the arm — the deadline then started a whole decision cycle
  late, which is precisely the silent defer AC8 exists to eliminate.
- `AwaitExternalWorkflow` (issue #757) — "the command suspends the caller",
  reaches the same arm unguarded, and was equally unlisted. Dropped, it is worse
  than a late deadline: no `ExternalAwaitRequested` is appended, so the caller
  parks on an await it never registered.

A `for_await: false` arm stays legal and is deliberately not listed: a fresh
`ctx.start_timer`/`reset` records `TimerStarted`, inserts no row and never
suspends, so it composes beside a local activity. `AcquireMutex` is also absent
by design — the dispatch arm already refuses to claim a batch containing one
(issue #691), so it never reaches the check and falls through to the fail-loud
generic path; the rustdoc now says so, since its absence otherwise reads as the
same kind of omission this round fixed.

**CI: no test reads `CLAUDE.md` any more.** Two guard suites had been rooted in
`CLAUDE.md`, which made a CI gate depend on an agent-guidance document nothing
warns you against editing. `562c781` ("fix: claude.md") trimmed that file from
10 629 lines to 19 and both began failing — on `trunk-dev` itself and on every
branch based on it. Because `Lint` fails, the whole `Test` job is skipped, so no
DB-backed suite in this repository ran in CI at all. Fixed here rather than
deferred, because it blocks this PR and is not this PR's to wait on:

- `performance_docs` (6 failing guards) cross-checks `docs/performance.md`
  against the verbatim claim-benchmark narrative. That prose lived in
  `docs/changelog.d/pr-786-claim-throughput-benchmark.md` until the 0.6.0
  collation sweep folded it into `CLAUDE.md`'s phase list and deleted the
  fragment. It now lives in `docs/performance-claim-benchmark.md`, a file the
  guards own — byte-for-byte as collated, so every assertion sees exactly what it
  saw before. The condensed `CHANGELOG.md` bullet is deliberately not the source:
  it drops the per-gate figures the guards compare.
- `migrating_from_temporal_docs::every_cited_issue_number_appears_in_claude_md`
  checked the guide's `#NNN` citations against `CLAUDE.md` as "the repository's
  own record of what has shipped". It now scans the engine source, `docs/`, and
  `CHANGELOG.md`, and is renamed `..._appears_in_the_repository`. That is the
  stronger record as well as the stabler one: it cannot be trimmed without
  deleting the code the citation describes. The citations were never wrong —
  all 45 resolve, and each of the ten that `CHANGELOG.md` alone misses is
  referenced across one to twenty source files.

Both remain load-bearing rather than merely green: the performance guards still
panic if the marker goes missing, and appending a fabricated `#987654` to the
migration guide still fails the citation guard by name.

**A push handler could still steal a race's signal (Codex round 7, P1;
issue #1252).** The mitigation above shipped as *evaluation ordering* — check
signal branches before the rest — on the reasoning that a sibling branch's scan
is what stashes the race's signal into `pending_signals`, so going first denies
the pump anything to claim. That reasoning was wrong, and the code comment and
review reply asserting it have been corrected.

Ordering only closes the window between branches. It cannot close the window
*before the first branch*, and `race_impl` always has calls there: it reads its
own `race:{seq}` open marker through `match_history`, and that read's
`prepare_match` sweep is itself what first stashes the cycle's `SignalReceived`
events. The pump fires immediately after it, with no branch yet evaluated. So
two shapes stayed broken:

- a **signal-only** race, `race().signal("abort")`, with a handler registered
  for `abort` — the open-marker read's pump takes the signal, no branch
  resolves, and replay reports non-determinism against the recorded
  `race_winner` marker;
- a **two-signal-branch** race where branch 0's scan crosses branch 1's signal
  non-consumingly and the pump following branch 0's own matcher call claims it
  before branch 1 runs.

The mixed activity-vs-signal shape already covered by
`race_mixed_signal_is_not_stolen_by_a_push_handler` passed throughout — which is
why ordering looked sufficient. Both new shapes are pinned as regression tests
and were confirmed failing first.

No history-derived reservation can fix this. The #476 index works because
`__signal_timeout:{seq}:{name}` encodes the signal name; a general race records
only `race:{seq}` and `race_winner:{seq}`, which name no branch, so there is
nothing to key an index off. The fix is therefore to defer the pump rather than
to reserve events: `WorkflowContext` gains a `signal_pump_hold` counter that
`pump_signal_handlers` checks and returns early on, raised by `race_impl` before
the open-marker read and released after Phase A, at which point one explicit
`flush_pending_signal_handlers` dispatches whatever no branch claimed. A counter
rather than a flag so nesting cannot clear an outer hold; an RAII guard so
`race_impl`'s several early `return Err(..)` paths cannot leak one.

Nothing is dropped — a handler whose signal no race branch wanted still fires,
one cycle position later than an unheld pump would have run it. Deferring is
also the only available point: the events only become visible at the very read
whose pump is suppressed, so there is no earlier moment at which to dispatch
them.

**Invariants.** **Zero new `WorkflowEvent` variants and no migration** — the
batch composes existing events (`ActivityScheduled`, `TimerStarted`,
`SignalReceived`, `ChildWorkflowStarted`, …) at their command-emission positions
through the shared `build_suspension_events`, which the replay matcher's
positional cursor already tolerates. Append-only preserved; Postgres-only.
Pre-existing parked tasks need no migration.

**Test evidence.**

- `worker.rs` unit tests: the extractor's accept matrix (all six named
  compositions plus parallel timers and the two fan-out stale-re-park shapes),
  its reject matrix (**every** kind in the reject arm, not a sample),
  bookkeeping tolerance asserted on the resolved buckets rather than
  `is_some`, the history-cap event-count arm, the duplicate-`StartTimer`-id
  guard, and the local-activity conflict report — including the awaited
  cancellable timer and the external await, with the non-awaiting
  `ArmTimer { for_await: false }` pinned as still legal so the rejection cannot
  creep into bookkeeping. AC6's ordering invariant is
  pinned by a source-level assertion that the mixed arm is the LAST arm of the
  dispatch chain — the extractors matching proves nothing about routing, and
  the mixed extractor is a superset of most legacy shapes, so a reorder would
  silently move every existing batch onto a different persist function.
- `context.rs` unit tests: mixed dispatch in one batch, the deterministic
  `__race:` id and its per-iteration freshness in a loop, activity-beats-timer /
  timer-beats-activity / signal-beats-activity / child-beats-timer winners with
  the correct `CancelRaceLosers` teardown per kind, branch-count
  non-determinism, a pin that the #476 timer+signal pair still takes the legacy
  path, and one regression test per review finding: a signal branch crossing
  more than one sibling event, a losing signal branch not stealing a later
  wait, a push handler not stealing a mixed race's signal — nor a signal-only
  race's, nor a second signal branch's (issue #1252) — and two timer branches
  advancing the clock to the max deadline rather than the sum.
- `tests/integration/mixed_suspension_tests.rs` (DB-backed, real worker loop,
  wired into `.github/ci/integration-suites.txt` so it actually runs): the ≥ 6
  composition matrix end-to-end — activity×timer both directions,
  activity×signal, child×timer both directions, child×signal, activity×child,
  the three-way activity×timer×signal resolved by its activity AND by its
  signal — plus `join!` wait-all over an activity and a durable timer, parallel
  timers parking at the earliest deadline, and the AC8 local-activity rejection
  failing loudly with no partial durable trace. Asserts both branches' events
  land in the same batch, losing timers rows are deleted and losing children
  are `CANCELLED`, and the history composes only pre-existing event variants.
- `tests/integration/replayer_tests.rs`: every composition replays clean in both
  directions, **1,000 randomized event-arrival orderings with 0 divergences**
  (the #476/#779 success-metric precedent), `join!` replays identically for both
  arrival orders, and an in-flight mixed race at the recorded frontier is a
  healthy suspend rather than a false non-determinism. Plain
  `join!(wait_for_signal, activity)` and `join!(wait_for_signal, child)` replay
  for both arrival orders, and the same shape parked with the signal
  undelivered is a healthy canary suspend. A signal wait joined with a durable
  TIMER parks on its first wake at three sampling points — before either branch
  resolves, at the persistence frontier of the three-way
  `join!(signal, timer, activity)`, and after the activity sibling has resolved
  with the timer still armed — while the stray-timer pin keeps diverging. The
  sweep genuinely
  permutes: it moves each losing branch's in-flight progress event to either
  side of the winner's terminal (the #1126 straddling-frontier case) and
  shuffles the teardown order, holding fixed only what is not arrival order —
  the atomically-written dispatch block, and which branch terminal is first.
  Each ordering is replayed twice and the two reports must agree, since
  "replays deterministically" is a statement about reproducibility.

**Known limitation (documented, not introduced here).** A workflow that
terminates while a durable timer it armed is still pending leaves an unfired
`harvest_timers` row, which `retention.rs`'s collection guard treats as a live
dependency. A mixed batch widens the ways to reach that state (a sibling
branch's failure can end the workflow while the timer is still armed), though
the class already existed for `receive_signal_timeout` and a cancelled/terminated
`ctx.timer` sleep. Sealing pending timer rows on the terminal transition is a
cross-cutting change to the completion/failure/cancel paths and is deliberately
out of scope here.
