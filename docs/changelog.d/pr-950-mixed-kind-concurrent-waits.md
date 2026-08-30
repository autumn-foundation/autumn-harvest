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
  untouched.
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

**Invariants.** **Zero new `WorkflowEvent` variants and no migration** — the
batch composes existing events (`ActivityScheduled`, `TimerStarted`,
`SignalReceived`, `ChildWorkflowStarted`, …) at their command-emission positions
through the shared `build_suspension_events`, which the replay matcher's
positional cursor already tolerates. Append-only preserved; Postgres-only.
Pre-existing parked tasks need no migration.

**Test evidence.**

- `worker.rs` unit tests: the extractor's accept matrix (all six named
  compositions plus parallel timers and the two fan-out stale-re-park shapes),
  its reject matrix (`AcquireMutex`, `ScheduleExternalActivity`,
  bookkeeping-only, empty), a routing pin asserting every legacy shape is still
  claimed by its own arm, and the local-activity conflict report.
- `context.rs` unit tests: mixed dispatch in one batch, the deterministic
  `__race:` id, activity-beats-timer / timer-beats-activity /
  signal-beats-activity / child-beats-timer winners with the correct
  `CancelRaceLosers` teardown per kind, branch-count non-determinism, and a pin
  that the #476 timer+signal pair still takes the legacy path.
- `tests/integration/mixed_suspension_tests.rs` (DB-backed, real worker loop):
  the ≥ 6 composition matrix end-to-end — activity×timer both directions,
  activity×signal, child×timer, child×signal, activity×child, the three-way
  activity×timer×signal — plus `join!` wait-all over an activity and a durable
  timer. Asserts both branches' events land in the same batch, losers are
  durably torn down, and the history composes only pre-existing event variants.
- `tests/integration/replayer_tests.rs`: every composition replays clean in both
  directions, **1,000 randomized event-arrival orderings with 0 divergences**
  (the #476/#779 success-metric precedent), `join!` replays identically for both
  arrival orders, and an in-flight mixed race at the recorded frontier is a
  healthy suspend rather than a false non-determinism.
