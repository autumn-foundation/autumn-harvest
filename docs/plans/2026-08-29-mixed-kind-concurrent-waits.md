# Mixed-kind concurrent waits in one suspension batch (issue #950)

Design plan produced ahead of a red/green/refactor TDD implementation.

## 1. Problem restated

`handle_suspended_workflow` in `autumn-harvest/src/worker.rs` dispatches a
suspension batch by pattern-matching a **closed set of homogeneous shapes**:

| Predicate / extractor | Shape |
| --- | --- |
| `should_requeue_signal_wait` | N `WaitForSignal` (+ external ops) |
| `only_bookkeeping_commands` | markers / side-effects only |
| `extract_all_scheduled_activities` | N `ScheduleActivity` |
| `extract_all_activity_waits` | N `WaitForActivity` |
| `extract_child_timeout_race` | 1 `StartChildWorkflow` + 1 `__child_timeout:` `StartTimer` |
| `extract_started_timer_for_suspension` | 1 `StartTimer` (+ `WaitForSignal`) |
| `extract_all_started_child_workflows` | N `StartChildWorkflow` |
| `extract_single_schedule_external_activity` | 1 `ScheduleExternalActivity` |
| `should_handle_mutex_acquire` | 1 `AcquireMutex` (solo) |

Anything else falls to `suspended_workflow_error` → terminal workflow failure.

Consequences:

* `join!(ctx.execute_activity(..), ctx.timer(..))` terminally fails the workflow.
* `ctx.race()` rejects mixed branch kinds up-front with `HarvestError::Config`
  (`race_impl`) precisely *because* the worker cannot persist the batch.
* The documented fan-out limitation (`child_fanout_tests.rs`) — a stale re-park
  command leaking into the next batch — kills a workflow that merely caught a
  fan-out error and continued.

The event model already supports all of this: every needed `WorkflowEvent`
variant exists and the replay matcher tolerates interleaved sibling events at
their recorded positions. **The gap is entirely in the worker's dispatcher.**

## 2. Brainstorming — candidate approaches

1. **Add one more hard-wired shape per requested combination.** Extend the
   `if/else if` chain with `extract_activity_timer_race`,
   `extract_activity_signal_race`, `extract_child_signal_race`, … Cheap per
   shape, but combinatorial (2^4 kinds × cardinalities) and each new arm
   duplicates the whole park/wake dance.
2. **Generalized mixed-batch extractor + one unified persist function.**
   Classify the batch into per-kind buckets; persist all buckets in one
   transaction reusing the existing per-kind primitives; park/reschedule once.
   Additive: placed *after* the existing arms so every current shape is
   untouched.
3. **Rewrite the dispatcher as a per-command visitor**, dropping the shape
   arms entirely. Cleanest end-state, but rewrites the most review-hardened
   code in the engine (9+ documented rounds of wake-race fixes) with no
   byte-for-byte compatibility story. Violates AC6's spirit.
4. **Command-graph IR**: lower commands into a persistence plan struct, then
   execute the plan. Nice, but the plan struct is the same work as (2) plus an
   abstraction nobody else consumes yet.
5. **Push the problem to the context layer**: make `join!`/`race` serialize the
   branches so only one durable command is ever emitted per cycle. Destroys the
   concurrency semantics the issue is about (a "concurrent" activity+timer
   would become sequential). Rejected outright.
6. **Split into N suspension cycles** (persist one kind, re-park, persist the
   next). Violates the AC1 "one transaction" requirement and multiplies
   decision cycles.
7. **New `SuspendBatch` event variant** recording the composite shape.
   Explicitly forbidden by AC3 (zero new event variants).

**Chosen: (2)**, with (3) noted as a follow-up refactor once the mixed path has
soaked. (2) is the only option that satisfies AC1 (one transaction), AC3 (zero
new events) and AC6 (existing shapes byte-for-byte identical) simultaneously.

## 3. Reverse brainstorming — how would we make this fail?

Deliberately enumerating failure modes, then designing them out:

| How to break it | Mitigation in the design |
| --- | --- |
| Silently drop a command kind we forgot to handle (e.g. `AcquireMutex`, `RunLocalActivity`, `ScheduleExternalActivity`) | The extractor is an **exhaustive `match` with an explicit reject arm** — any unknown/unsupported command returns `None`, falling through to the existing fail-loud path. No `_ => {}` catch-all. |
| Change the event order of an existing shape, nd-blocking in-flight executions | Mixed arm is placed **last**, after every existing extractor. Every batch that matches an old shape still takes the old arm. Backward-compat test asserts this ordering explicitly. |
| Reuse an event id and hit `UNIQUE(workflow_exec_id, event_id)` | Use `append_single_event` (parent-row `FOR UPDATE` + `MAX(event_id)` re-read per insert), like `persist_all_started_child_workflows` / `persist_child_timeout_race`. A batch containing a child or a `CancelRaceLosers` can race a sibling's terminal append. Re-read `next_event_id_for` before the race-loser sweep. |
| Lose a wake: activity completes while the cycle is still RUNNING | `park_workflow_task` returns `had_wake_requested` (atomic read-and-clear). Must be honoured on **both** the park and the timer-reschedule sub-paths. The timer sub-path uses `reschedule_task`, which does **not** read `wake_requested` — so the mixed path reads and clears it explicitly and self-wakes. |
| Lose a wake: signal arrives between park and commit | Re-check `load_pending_signals` after commit when the batch has any `WaitForSignal`, mirroring `persist_signal_wait_park`. |
| Lose a wake: sibling activity/child already terminal | Post-park re-check `has_activity_terminal_event` for waited activity ids and child terminal states, mirroring `persist_activity_wait_park` / `persist_all_started_child_workflows`. |
| Timer branch sleeps until `fires_at` even though a sibling resolved | Park as `PENDING @ min(fires_at)` **plus** the `mixed_signal_suspension` sentinel, so `primary_repend_workflow_task_query`'s second arm pulls it forward on any wake. |
| A batch with a timer *and* nothing else durable regresses to the mixed path | Not possible: `extract_started_timer_for_suspension` matches first. |
| Multiple `StartTimer`s in one batch (parallel timers) | Supported: insert each row, park at the **minimum** `fires_at`. Each timer's `TimerStarted` is emitted at its own command position. |
| Terminally fail a healthy parent over another tenant's quota | Catch `QuotaExceeded` at the transaction boundary and park+wake, exactly like the two child paths. |
| Terminally fail on a capability miss instead of releasing for a capable peer | Pre-resolve activity handlers and child workflow registrations *before* any insert, raising `HarvestError::HandlerNotRegistered { phase: AfterHandler }` — same as the homogeneous paths. |
| Break the #476 timer+signal byte-for-byte shape | That shape never reaches the mixed arm. Regression test pins it. |
| Break #678's mixed timer + inline-external wake | `resolved_inline_external` is threaded into the mixed path too, forcing the `mixed_signal_suspension` stamp. |
| Replay diverges because live and replay disagree on event order | All events built through the shared `build_suspension_events` in **command emission order** — the single source of order truth already used by every other path. |
| A local activity in a mixed batch silently defers its siblings | AC8: reject with an immediate typed error (see §5). |
| `ctx.race()` accepts a shape the worker still can't persist | The race builder's accepted kinds are a strict subset of the mixed extractor's accepted kinds; a unit test asserts every race dispatch shape is extractor-accepted. |

## 4. Six hats

**White (facts).** `worker.rs` is 31k lines; `handle_suspended_workflow` is a
9-arm if/else. All needed events exist. `CancelRaceLosers` already carries a
`timers` field. `build_suspension_events` already interleaves by command
position. `park_workflow_task` already returns `had_wake_requested`.
`primary_repend_workflow_task_query` already re-pends `PENDING` rows stamped
`mixed_signal_suspension`. External signal/cancel/await commands are stripped
before `handle_suspended_workflow` by `split_mixed_signal_batch`.

**Red (feelings).** This is the scariest file in the repo. The instinct is to
touch nothing that already works. That instinct is *right* and shapes the whole
design: additive last arm, zero edits to existing extractors or persist
functions. The other worry is test cost — DB-backed worker tests are slow;
mitigate with pure unit tests for the extractor and replay tests for
determinism, reserving Postgres tests for the end-to-end matrix.

**Yellow (upside).** One new arm removes an entire class of runtime
"unsupported commands" failures, unblocks hedging/deadline/abort-signal
patterns, closes the fan-out stale-re-park limitation for free, and lets
`ctx.race()` drop its apology in the docs. It closes Harvest's most visible
expressiveness gap vs Temporal/Restate.

**Black (risks).** (a) Lost wakes — the historical bug class here; addressed by
the reverse-brainstorm table and by *reusing*, not reimplementing, each
post-park re-check. (b) Event-id collisions under concurrent sibling terminals
— addressed with `append_single_event`. (c) A batch that "looks" mixed but is
really an old shape taking a new code path — addressed by arm ordering + a
pinning test per legacy shape. (d) Scope creep into `ScheduleExternalActivity`
and `AcquireMutex` — deliberately excluded, still fail-loud.

**Green (creativity).** Rather than N persist functions, the mixed path is a
*single* transaction that walks the buckets in a fixed order (timers resolved →
events appended → detached spawns → race-loser cancellations → activity
enqueues → child rows → park/reschedule). The park decision reduces to one
boolean: *does this batch contain a `StartTimer`?* If yes, `reschedule_task` at
`min(fires_at)` + sentinel; if no, `park_workflow_task`. Everything else is
kind-independent.

**Blue (process).** TDD: red tests first (extractor unit tests, dispatcher
routing tests, race-builder tests, replay-determinism tests, then DB-backed
end-to-end matrix), then implement, then refactor, then multi-angle agent
review, then an AC evidence table.

## 5. Decisions

* **D1 — Placement.** New arm `extract_mixed_suspension_batch` sits between
  `should_handle_mutex_acquire` and the fail-loud `else`. Every existing shape
  keeps its current arm; AC6 holds by construction.
* **D2 — Accepted kinds.** `ScheduleActivity`, `WaitForActivity`, `StartTimer`,
  `WaitForSignal`, `StartChildWorkflow`, plus all bookkeeping already tolerated
  elsewhere (`RecordMarker`, `RecordSideEffect`, `RecordUpdateResult`,
  `UpsertSearchAttributes`, `SetCurrentDetails`, `PublishProgress`,
  `RecordLog`, `SpawnDetachedChildWorkflow`, `CancelRaceLosers`, `ArmTimer`,
  `CancelTimer`, `ReleaseMutex`). Everything else → `None` → fail loud.
  Requires **at least two distinct wait kinds** so it can never shadow a
  homogeneous shape.
* **D3 — One transaction.** All persistence for the batch commits atomically
  (AC1). Post-commit work is limited to deferred completion-trigger spawns and
  the self-wake re-checks, exactly as the existing paths do.
* **D4 — Park semantics.** Timer present → `reschedule_task(min fires_at)` +
  `activity_name = 'mixed_signal_suspension'`; no timer → `park_workflow_task`.
  `wake_requested` is read-and-cleared in both sub-paths and honoured.
* **D5 — Race builder.** `race_impl` accepts any combination. Timer branches
  dispatch `StartTimer` with a reserved `__race:{seq}:{index}` id and are torn
  down via `CancelRaceLosers.timers` when they lose; signal branches dispatch
  `WaitForSignal` and need no teardown. The legacy exactly-one-timer +
  exactly-one-signal pair keeps delegating to `race_timer_signal_impl` so its
  recorded histories and role-based indices are unchanged (AC6).
* **D6 — Local activities (AC8).** A `RunLocalActivity` co-batched with any
  sibling *wait/dispatch* command is rejected with an immediate typed
  `HarvestError::Config`, replacing the silent defer in
  `extract_run_local_activity`. Rationale: an inline local activity resolves
  *within* the decision cycle and appends its own terminal, so joining it with
  durable waits would require re-entering the body mid-batch — a much larger
  change than this issue's scope, and silently arming a deadline late is the
  worse failure. Documented in the local-activity docs and pinned by a test.
* **D7 — Zero event-schema change.** No new `WorkflowEvent` variant, no
  migration (AC3).

## 6. Test plan

* Pure unit (no DB): extractor accept/reject matrix; arm-ordering pins for all
  nine legacy shapes; `min fires_at` selection; local-activity rejection;
  race-builder shape acceptance and command emission.
* Replay (no DB): a fixture per composition replayed under `WorkflowReplayer`,
  plus a **1,000-iteration randomized event-ordering sweep** per the #476/#779
  precedent, asserting 0 divergences.
* DB-backed worker (`feature = "db"`, testcontainers Postgres): the ≥ 6
  composition matrix (activity×timer, activity×signal, child×timer,
  child×signal, activity×child, activity×timer×signal) end-to-end through the
  real worker, plus a one-transaction atomicity assertion and a
  backward-compatibility assertion that legacy shapes persist unchanged.
* Fan-out: flip `child_fanout_tests.rs`'s known-limitation pins to positive
  assertions (AC7).
