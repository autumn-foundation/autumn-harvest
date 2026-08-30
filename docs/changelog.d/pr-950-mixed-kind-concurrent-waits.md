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
  divergence. The solo `wait_for_signal` guard (issue #768 round 13) is
  untouched. The scan is **bounded by the race's own `race_winner:{seq}`
  marker**, so a *losing* signal branch can never consume a `SignalReceived`
  delivered after the race resolved — that signal belongs to a later
  `ctx.wait_for_signal`, which would otherwise park on a signal already
  delivered.
- `ctx.race()` evaluates **signal branches first**. `pump_signal_handlers` runs
  after every `match_history` call, and a sibling branch's matcher stashes the
  race's signal on its way past it, so a push handler registered for the same
  name would claim it before the race's own branch was checked — and the race
  could then never re-resolve its recorded winner. The #476 reservation index
  keys off the `__signal_timeout:{seq}:{name}` timer id, which encodes the
  signal name; a mixed race's `__race:{seq}:{index}` id encodes none, and an
  activity-vs-signal race arms no timer at all, so that index cannot cover this
  shape. Claiming first does, and it is order-independent: the branch INDEX
  still decides the tie-break.
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
  guard, and the local-activity conflict report. AC6's ordering invariant is
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
  wait, a push handler not stealing a mixed race's signal, and two timer
  branches advancing the clock to the max deadline rather than the sum.
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
  healthy suspend rather than a false non-determinism. The sweep genuinely
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
