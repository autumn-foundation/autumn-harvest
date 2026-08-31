# Make failing decision cycles replay-clean and lossless (issue #952)

Design plan produced ahead of a red/green/refactor TDD implementation.

## 1. Problem restated

A workflow's *failing* decision cycle is second-class in two places.

### Seam 1 — the matcher diverges on a terminal `WorkflowFailed`

`HistoryMatcher::new` (`autumn-harvest/src/replay.rs`) pre-marks pause/resume
events transparent (#383) and, for DLQ redrive (#510), the `WorkflowFailed`
*immediately preceding* a `WorkflowRedriven`. A **bare trailing**
`WorkflowFailed` is deliberately left opaque, so any `match_*` call whose cursor
reaches it reports `Diverged { actual: "WorkflowFailed" }` instead of
recognising "this run is failing". Two tests pin the consequence:

* `replayer_tests::known_limitation_early_config_dependent_failure_does_not_replay_cleanly`
  (plain `spawn_child_workflow_raw` with an oversize payload)
* `replayer_tests::replayer_diverges_at_marker_not_at_a_phantom_child_for_payload_cap_failure`

Even with the matcher fixed, three more layers would still report a divergence
for the same history:

* `WorkflowContext::check_strict_replay_no_match` turns the resulting `NoMatch`
  into an `early completion mismatch` non-determinism error under strict replay;
* `executor::run_strict_with_ctx`'s completed-arm "new commands emitted beyond
  recorded history" check fires when a *fixed* build now completes past the
  recorded failure point;
* `testing::outcome_to_report` maps a `Failed` outcome to
  `ReplayStatus::WorkflowFailed` and a `Suspended` outcome to
  `NonDeterminismDetected`, so even a perfectly faithful replay of a failed run
  is never `ReplaySucceeded`.

### Seam 2 — `persist_terminal_outcome_commands` silently drops dispatches

The terminal-with-pending-commands path persists an *allowlist*: update results,
search attributes, current details, durable logs, progress chunks, timer
lifecycle, race-loser teardown, and the
`RecordMarker` / `RecordSideEffect` / `SpawnDetachedChildWorkflow` triple that
`pre_suspension_events_from_commands` knows how to turn into events. Every other
command in the drained batch — most importantly `StartChildWorkflow` and
`ScheduleActivity` — falls through the `filter_map` and disappears with no
event, no row, and no log line.

A batch reaches that path whenever a workflow dispatches work concurrently and a
*sibling* branch returns `Err` in the same poll, e.g.

```rust
futures::try_join!(
    ctx.spawn_child_workflow_fan_out_raw(three_children), // 3 commands pushed, all parked
    async { Err(HarvestError::Config("budget exceeded".into())) },
)?
```

The three dispatches are real: the workflow code asked for them. The persisted
history says they never happened.

## 2. Brainstorming — candidate approaches

**Seam 1**

1. Mark **every** `WorkflowFailed` transparent unconditionally. One line, but it
   also blesses the mid-history `WorkflowFailed` of a workflow-level retry chain
   and of a redrive whose reopened cycle re-issues real commands — the exact
   case #510 hand-tuned.
2. Mark only a **trailing** `WorkflowFailed` transparent (nothing but
   post-terminal bookkeeping after it). Subsumes #510's redrive rule as a
   sibling rather than replacing it, and cannot affect any history that
   continues past the failure.
3. Teach every `match_*` arm to special-case `WorkflowFailed` at the cursor.
   ~20 call sites, each a chance to get it wrong; the transparency set already
   exists precisely to avoid this.
4. Add a `ReplayStatus::ReplayedFailedRun` variant so failed histories get their
   own verdict. New public API surface, and every existing
   `matches!(status, ReplaySucceeded)` CI gate in the wild would have to learn
   about it — the opposite of "zero false positives out of the box".
5. Strip the trailing `WorkflowFailed` before constructing the matcher. Loses
   the information that the run *is* failing, which is exactly what the frontier
   checks need.

**Seam 2**

6. Make the awaited children genuinely start (issue option (i)): child rows +
   task enqueue + quota admission from inside the terminal transaction, running
   as orphans of a dead parent.
7. Record the dispatch as failed-before-start (issue option (ii)):
   `ChildWorkflowStarted` + `ChildWorkflowFailed { synthetic reason }`, no rows,
   no enqueue.
8. Record only `ChildWorkflowStarted` and leave the branch in progress forever.
9. Keep dropping them, but fail the whole cycle loudly (`Unsupported`) the way
   `autumn-harvest-sqlite`'s `persist_terminal_pending_commands` does.
10. Reuse `pre_suspension_events_from_commands` for the terminal path with the
    full suspension persistence (enqueue, rows, timers) — i.e. treat a terminal
    batch exactly like a suspension batch.

## 3. Reverse brainstorming — how would we *guarantee* this stays broken?

* Make the transparency rule depend on a scan the cursor has already passed, so
  it silently stops applying once history grows.
* Fix the matcher and forget the three downstream layers, so the named tests
  still fail and the "fix" looks like a no-op.
* Apply the new persistence to **all** terminal outcomes, so a *completed*
  history suddenly contains synthetic `ActivityFailed` events; replay of a
  healthy run then resolves a branch that live-parked, flips a `select!`, and
  the strict completed-arm checks report drift on green runs. (This is the
  single highest-risk way to "fix" seam 2 — hence the Failed-only scope below.)
* Emit `ChildWorkflowStarted` for a child that was merely *re-parked* (the
  `HistoryMatch::ChildInProgress` arm re-emits `StartChildWorkflow` with the
  **existing** `child_id`), duplicating a start and synthesising a failure for a
  child that is genuinely running.
* Extend the allowlist by hand-editing a `matches!` list, so the next
  `WorkflowCommand` variant is dropped in silence again.
* Invent a new `WorkflowEvent` variant or a new `reason_code` string, breaking
  the contract the issue explicitly freezes.
* Write the audit as prose in a doc comment with no test, so it rots.

Each of these is inverted into a concrete guard in §6.

## 4. Six hats

* **White (facts).** 26 `WorkflowCommand` variants; 5 are persisted as
  pre-terminal events today (markers, side effects, detached spawns, and the two
  timer-lifecycle commands), 7 through side paths, the rest dropped. Terminal
  lifecycle events are already excused from `has_non_lifecycle_unconsumed`.
  `WorkflowRetryScheduled` / `ChildWorkflowCascadeApplied` are appended *after*
  a terminal event. `ChildWorkflowFailed` and `ActivityFailed` already carry a
  synthetic-failure precedent: `apply_race_loser_cancellations` writes
  `ActivityFailed { error: "lost race to a sibling branch", non_retryable: true }`.
* **Red (instinct).** Emitting `ChildWorkflowStarted` for a child that never got
  an execution row feels like a lie — until you notice the alternative is a
  history that omits the dispatch entirely, and that the parent's own
  `WorkflowFailed` lands two events later. Recording *and* immediately marking
  it failed is the honest shape.
* **Black (risks).** (a) Transparency could mask genuine drift *before* the
  failure — mitigated: only the trailing terminal block becomes transparent,
  every earlier event is still matched positionally. (b) Synthetic terminals
  change what a *replayed* branch observes: code that catches a child failure
  and compensates will take a different path on replay. Mitigated by the
  frontier rule (post-failure commands are unverifiable by construction) and
  documented for the residual case where the compensation collides with a later
  recorded dispatch. (c) Applying (b) to completed runs would be a live
  regression — hence Failed-only.
* **Yellow (upside).** The two named limitation tests flip; the deploy gate
  (#798) and the divergence-diagnosis work (#614) inherit a trustworthy verdict
  on failed runs; post-mortems see the dispatched children; the exhaustive
  classifier makes the next command variant a compile error rather than a
  silent drop.
* **Green (creative).** The classifier doubles as living documentation: one
  `const fn` returning a policy enum, rendered into a table by a test that
  enumerates every variant.
* **Blue (process).** Red → green → refactor per seam, in this order: matcher
  transparency, frontier checks, report mapping, then persistence. Each step's
  red test is a `ReplaySucceeded`/event-count assertion written before the code.

## 5. Chosen design

### 5.1 Seam 1 — a failed run is verified up to its failure point

**One rule, four enforcement points:** *when a history's terminal block is a
`WorkflowFailed`, everything the workflow does after consuming the last
pre-terminal event is unverifiable and is therefore not a divergence.* A failing
cycle's history is truncated by construction — the commands it issued were never
turned into events — so there is nothing to compare against.

1. **`HistoryMatcher::new`** computes the *terminal-failure tail*: walk backwards
   over already-transparent events and post-terminal bookkeeping
   (`WorkflowRetryScheduled`, `ChildWorkflowCascadeApplied`); if the event landed
   on is `WorkflowFailed`, mark it **and every event after it** transparent.
   `prepare_match` then returns `false` at that point and every `match_*`
   answers `NoMatch` instead of `Diverged`. The #510 redrive rule is untouched
   (a `WorkflowRedriven` is not post-terminal bookkeeping, so a reopened run
   never has a tail) — both rules simply insert into the same set.
2. **`WorkflowContext::strict_replay_no_match_is_divergence`** additionally
   returns `false` at the *failing frontier* (a tail exists and no unconsumed
   non-transparent event remains). Both the returning form
   (`check_strict_replay_no_match`) and the deferred form used by the infallible
   built-ins share this predicate, so they cannot drift.
3. **`executor`'s completed-arm** "new commands emitted beyond recorded history"
   check is skipped at the failing frontier, so a build that *fixed* the
   config-dependent failure and now completes is not reported as drift.
4. **`testing::outcome_to_report`** maps, for a history with a terminal-failure
   tail: a non-ND `Failed` outcome → `ReplaySucceeded` (the run failed again —
   deterministic), and `Suspended` → `ReplaySucceeded` (the run parked where it
   previously failed). A **handler panic** and any ND-carrying failure are still
   reported, because those are real defects rather than a reproduced outcome.

### 5.2 Seam 2 — abandoned dispatches are recorded

`terminal_command_policy(&WorkflowCommand) -> TerminalCommandPolicy` is an
**exhaustive** `match` — the compile-time invariant that AC4 asks for. Policies:

| Policy | Meaning |
| --- | --- |
| `PreTerminalEvent` | already appended by the pre-terminal batch builder |
| `SidePath(&str)` | persisted outside `harvest_events` (column, table, notify, row mutation) |
| `AbandonedDispatch` | dispatched work the failing cycle abandoned — **new**: recorded as `*Started/Scheduled` + a synthetic terminal |
| `NoRecord(&str)` | records nothing, with the reason |
| `Unreachable(&str)` | cannot appear in a drained terminal batch |

`AbandonedDispatch` covers exactly the two kinds that would otherwise have
become **work other subsystems can see** — a child execution row, a task-queue
row:

* `StartChildWorkflow` → `ChildWorkflowStarted { child_id, workflow_name, input }`
  + `ChildWorkflowFailed { child_id, error: <synthetic>, non_retryable: true }`
* `ScheduleActivity` → `ActivityScheduled { activity_id, name, input, queue }`
  + `ActivityFailed { activity_id, error: <synthetic>, attempt: 1, error_type: "Error", non_retryable: true }`

Both use existing variants only (AC6). The synthetic terminal is what keeps the
record *resolvable* on replay: without it the branch would re-park forever.

Scope guards:

* **Failed outcomes only.** `Completed` / `ContinuedAsNew` keep today's exact
  behaviour; injecting synthetic failures into a strictly-verified completed
  history is the regression identified under Black/reverse-brainstorming.
* **Fresh dispatches only.** A `StartChildWorkflow` whose `child_id` already has
  an execution row is a re-park of a genuinely running child (the
  `ChildInProgress` arm) and is skipped — the same dedup the suspension path
  does. `ScheduleActivity` is fresh by construction (a re-park emits
  `WaitForActivity`), asserted by a test.
* **Emission order.** The events are interleaved at each command's position by
  the same `build_suspension_events` walker, so positional replay sees the order
  the live cycle emitted.

Everything else keeps today's behaviour with an explicit, tested policy —
notably `ReleaseMutex` (subsumed by `mutex::sweep_terminal_holder_and_wake` on
the terminal seal), `WaitForActivity` / `WaitForSignal` (no event on *any* path),
`AcquireMutex` (a grant that never happened records no `MutexGranted`), and
`Complete` / `Fail` (never pushed by the context — `Unreachable`).

## 6. Guards (inverted from §3)

| Failure mode | Guard |
| --- | --- |
| Rule stops applying as history grows | tail is computed once in `new` over the whole event list, plus a test with post-terminal `WorkflowRetryScheduled` |
| Only the matcher fixed | the two named tests assert `ReplaySucceeded` end-to-end through `WorkflowReplayer` |
| Synthetic failures on healthy runs | `Completed`/`ContinuedAsNew` untouched; test asserts a completed batch still drops dispatches |
| Duplicate start for a re-parked child | dedup on the existing execution row; unit test |
| Next command variant dropped silently | exhaustive `match` in `terminal_command_policy` (compile error) + a test enumerating all variants |
| New event variant / reason code | tests assert only existing variants are emitted; no `event.rs` change in the diff |
| Audit rots | the policy table is generated from the classifier by a test |

## 7. Test plan (red first)

Pure — `--no-default-features --features testing --test integration` and `--lib`:

1. `replay.rs`: trailing `WorkflowFailed` transparent; not transparent when
   followed by a real command event; redrive case still transparent; tail
   through `WorkflowRetryScheduled`.
2. `replayer_tests.rs`: the two named tests flip to `ReplaySucceeded`; a
   ≥5-shape fixture corpus (early config-dependent failure; failed-before-suspend
   with 1 child; with 3 children; failure after a marker; failure after a side
   effect) all report `ReplaySucceeded`; negative controls — a *completed*
   history with drift still reports `NonDeterminismDetected`, and drift
   *before* the failure point in a failed history still reports it.
3. `worker.rs` unit tests: the classifier table; the event builder emits
   `Started+Failed` per dispatched child in emission order (3 children → 3
   pairs); dedup skips an existing child; a `Completed` batch emits none;
   `ScheduleActivity` pair; every non-dispatch variant emits nothing.
4. End-to-end without a database: run the real `try_join!` workflow through
   `executor::run_workflow`, feed its **real** drained commands to the **real**
   event planner, assert 3 `ChildWorkflowStarted` events, then replay the
   resulting history through `WorkflowReplayer` and assert `ReplaySucceeded`.
   (The Docker-backed lane additionally exercises the DB path; the planner is
   the exact function that path calls.)

## 8. Review round 1 — findings and resolutions

Four independent review passes (replay semantics, worker persistence, API/docs,
test quality) ran against the finished implementation. Everything below is fixed
in this same change.

| Finding | Resolution |
| --- | --- |
| **P1** A park on a failing-tail history reported `ReplaySucceeded` even when it left recorded events unconsumed — the strict path had no unconsumed check, having leaned on `outcome_to_report` reporting *every* strict suspension | `run_strict_with_ctx`'s suspend arm now runs the canary path's `history_has_unconsumed_events()` check, so an EARLY park is an ND `Failed` and only a park at the failing frontier is absorbed |
| **P1** The `Failed` absorption keyed off the `"non-deterministic replay: "` error prefix, which a workflow that wraps the engine error (`.map_err(\|e\| format!(…))`) loses | Absorption is now gated on `non_deterministic_details.is_none()` — the authoritative signal every divergence latches through `nd_error` |
| **P1** DLQ redrive (#510) poisoning: the abandoned-dispatch records became ordinary history for the reopened run, which would read back "this dispatch failed" and could never re-issue it | `HistoryMatcher::new` marks abandoned-dispatch pairs transparent **in a redriven history**, so the reopened cycle emits them live exactly as it did pre-#952. Pinned by `a_redrive_makes_abandoned_dispatch_records_transparent` and its no-redrive twin |
| **P1** The child dedup asked "does a row exist", but the matcher's `ChildInProgress` is driven by HISTORY — the two disagree after a retention sweep or partial restore | `RecordedDispatchIds::from_history` (pure, computed once) is now consulted alongside the row check, and it covers `ScheduleActivity` too — making the "fresh by construction" argument mechanical |
| **P2** `is_replaying()`'s sealed-run clause did not reach the guards that lock the matcher directly (logger #379/#790, `publish_progress` #791, `info().is_replaying`) | All of them route through one `replay_suppresses_side_effects()` predicate |
| **P2** `version()` guarded its marker push on `is_replaying()` while `match_version` decided from the cursor — at a failing frontier it latched `patch_ids_recorded_this_cycle` without pushing | `version()` uses the cursor-only `at_history_frontier()`, the same question the matcher asked |
| **P2** The plugin's replay-diagnosis endpoint returned `clean` with `failure: null` for a reproduced failure — the operator's post-mortem surface lost the error | `report_to_response` surfaces `reproduced_failure` as the response's `failure` detail |
| **P2** The redrive backward scan stopped at post-terminal bookkeeping, leaving a retried-then-redriven run's terminal opaque | The scan skips it, using the same `is_post_terminal_bookkeeping` helper as the tail walk |
| **P2** `persist_terminal_outcome_commands` appended at a **pre-handler** `next_event_id` snapshot; a child terminal committing in between would collide on `UNIQUE(workflow_exec_id, event_id)` and roll the cycle's race-loser cancellations back with it | The id is re-read under the row lock the transaction already holds, mirroring `apply_race_loser_cancellations` |
| **P2** A `Completed`/`ContinuedAsNew` cycle still drops dispatches — correct, but silent | One `tracing::warn!` per cycle naming the count |
| **P2** `terminal_history_event_count` (the `history_size` gauge) did not count the new events the cap preflight counts | It does now, for failing terminals |
| **P3 / test quality** The 26-variant audit compared the table against a hand-written literal; a 27th variant would not move it | The count is derived from an exhaustive `workflow_command_variant_index` match — a new variant fails to compile, then fails the assertion until the table covers it |
| **P3 / test quality** The corpus asserted only `ReplaySucceeded`, which is reachable three ways; the 3-child fixture short-circuited on slot 0 so slots 1–2 were never verified | The fixture handler consumes every slot (`join_all`), the corpus asserts the reproduced failure, and a dedicated test asserts each slot resolves from its recorded synthetic terminal |
| **P3 / test quality** `patched()` at the failing frontier, the `is_replaying()` semantic change, the `Failed`-only scope gate, and the history dedup had no tests | One test each |

Two findings were reviewed and **deliberately not changed**, documented instead:

* **A failing replay that returns `Err` without reaching every recorded event is
  not reported.** A fail-fast fan-out is exactly that shape — the first branch to
  fail aborts its siblings — and the recorded run failed for the same reason.
  Requiring full consumption there would re-introduce the false positives on
  failed runs that this issue exists to remove. Stated explicitly in
  `docs/replay-verify.md`.
* **The offload-before-commit window** (`append_events_offloaded` uploads a blob
  before the transaction commits, so a rollback can orphan it) is pre-existing on
  the suspension path and unchanged in kind here.
